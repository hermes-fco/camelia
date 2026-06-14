#!/usr/bin/env raku
# 🌺 Camélia — Skill Store (JetStream-backed + in-memory index)
#
# CRUD for procedural skills. Workers and orchestrator query this.
# Content stored in JetStream (skill.data.<name>), metadata indexed
# in memory for fast listing/searching.
#
# Operations (via request-reply on skill.store.*):
#   get    { name }           → full skill object
#   set    { name, content, description?, tags? } → upsert
#   list   { tag? }           → array of { name, description, tags }
#   delete { name }           → ok/error
#   search { query }          → array of matching skills

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;

my $nats-url     = %*ENV<NATS_URL>     // 'nats://127.0.0.1:4222';
my $service-name = %*ENV<SERVICE_NAME>  // 'skill-store';

# ── In-memory skill index ──
my %index;  # name → { description, tags, updated_at }

# ── Connect NATS ──
note "🟡 Skill-Store connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start;
$nats.connect;
note "🟢 Skill-Store connected.";

# ── JetStream setup ──
note "📦 Setting up SKILLS stream...";
my $stream = Nats::Stream.new:
    :$nats,
    :name<SKILLS>,
    :subjects(['skill.data.>']),
    :retention<limits>,
    :max-msgs-per-subject(1),
    ;
my $s-supply = $stream.create;
my $s-msg = await $s-supply.Promise;
note $s-msg ?? "  ✅ Stream ready" !! "  ⚠️ Stream creation returned no message";
note "✅ JetStream ready.";

# ── GET: read full skill from JetStream ──
sub skill-get(Str $name --> Hash) {
    my $sub = $nats.subscribe: my $inbox = "_INBOX.sg." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: "\$JS.API.STREAM.MSG.GET.SKILLS", to-json({
        :last_by_subj("skill.data.{$name}"),
    }), :reply-to($inbox);
    await Promise.anyof: $p, Promise.in(5);
    $nats.unsubscribe: $sub;
    return { :error("timeout") } unless $p.so;
    my $msg = $p.result;
    return { :error("no payload") } unless $msg && $msg.payload;
    my $resp = try from-json($msg.payload);
    if $! || !$resp<message> {
        return { :error("Skill not found: {$name}") };
    }
    my $data = try from-json($resp<message><data>) // $resp<message><data>;
    return $data if $data ~~ Hash;
    return { :error("Skill not found: {$name}") };
}

# ── SET: store in JetStream + update index ──
sub skill-set(Str $name, Str $content, Str :$description = '', :@tags = []) {
    my $now = DateTime.now.Str;
    my %skill = :$name, :$content, :$description, :tags(@tags),
                :updated_at($now);

    # Preserve created_at from existing skill
    my %existing = skill-get($name);
    %skill<created_at> = %existing<created_at> if %existing<created_at> && !%existing<error>;

    $nats.publish: "skill.data.{$name}", to-json(%skill);

    # Update in-memory index
    %index{$name} = { :$description, :tags(@tags), :updated_at($now) };

    return { :ok(True), :$name, :message("Skill '{$name}' stored") };
}

# ── DELETE ──
sub skill-delete(Str $name --> Hash) {
    my $sub = $nats.subscribe: my $inbox = "_INBOX.sd." ~ (^1_000_000).pick, :1max-messages;
    my $p   = $sub.supply.head.Promise;
    $nats.publish: "\$JS.API.STREAM.PURGE.SKILLS", to-json({
        :filter("skill.data.{$name}"),
    }), :reply-to($inbox);
    await Promise.anyof: $p, Promise.in(5);
    $nats.unsubscribe: $sub;

    %index{$name}:delete;
    return { :ok(True), :$name, :message("Skill '{$name}' deleted") };
}

# ── LIST: from in-memory index ──
sub skill-list(Str :$tag --> Hash) {
    my @skills;
    for %index.kv -> $name, $meta {
        if !$tag || ($meta<tags> // []).grep($tag) {
            @skills.push: {
                :$name,
                :description($meta<description> // ''),
                :tags($meta<tags> // []),
                :updated_at($meta<updated_at> // ''),
            };
        }
    }
    return { :ok(True), :skills(@skills), :count(+@skills) };
}

# ── SEARCH: substring match on name/description/tags ──
sub skill-search(Str $query --> Hash) {
    my $q = $query.lc;
    my @matches;
    for %index.kv -> $name, $meta {
        if $name.lc.contains($q) ||
           ($meta<description> // '').lc.contains($q) ||
           ($meta<tags> // []).grep({ .lc.contains($q) }) {
            @matches.push: {
                :$name,
                :description($meta<description> // ''),
                :tags($meta<tags> // []),
                :updated_at($meta<updated_at> // ''),
            };
        }
    }
    return { :ok(True), :skills(@matches), :count(+@matches), :$query };
}

# ── Subscribe to operations ──
my $get-sub    = $nats.subscribe: 'skill.store.get';
my $set-sub    = $nats.subscribe: 'skill.store.set';
my $list-sub   = $nats.subscribe: 'skill.store.list';
my $delete-sub = $nats.subscribe: 'skill.store.delete';
my $search-sub = $nats.subscribe: 'skill.store.search';
my $health-sub = $nats.subscribe: 'health.check.skill.store';
note "🟢 Listening on skill.store.*";

# ── React loop ──
react {
    whenever $get-sub.supply -> $msg {
        next unless $msg.payload && $msg.?reply-to;
        my %req = try from-json($msg.payload) // {};
        my $name = %req<name> // '';
        start {
            my %resp := skill-get($name);
            $nats.publish: $msg.reply-to, to-json(%resp);
        }
    }

    whenever $set-sub.supply -> $msg {
        next unless $msg.payload && $msg.?reply-to;
        my %req = try from-json($msg.payload) // {};
        my $name    = %req<name>    // '';
        my $content = %req<content> // '';
        unless $name && $content {
            $nats.publish: $msg.reply-to, to-json({ :error("name and content required") });
            next;
        }
        start {
            my %resp := skill-set($name, $content,
                :description(%req<description> // ''),
                :tags(%req<tags> // []),
            );
            $nats.publish: $msg.reply-to, to-json(%resp);
            note %resp<error> ?? "  ❌ set {$name}: {%resp<error>}" !! "  💾 Skill '{$name}' stored";
        }
    }

    whenever $list-sub.supply -> $msg {
        next unless $msg.?reply-to;
        my %req = try from-json($msg.payload // '{}') // {};
        start {
            my %resp := skill-list(|(:tag(%req<tag>) if %req<tag>:exists));
            $nats.publish: $msg.reply-to, to-json(%resp);
        }
    }

    whenever $delete-sub.supply -> $msg {
        next unless $msg.payload && $msg.?reply-to;
        my %req = try from-json($msg.payload) // {};
        my $name = %req<name> // '';
        start {
            my %resp := skill-delete($name);
            $nats.publish: $msg.reply-to, to-json(%resp);
        }
    }

    whenever $search-sub.supply -> $msg {
        next unless $msg.payload && $msg.?reply-to;
        my %req = try from-json($msg.payload) // {};
        my $query = %req<query> // '';
        start {
            my %resp := skill-search($query);
            $nats.publish: $msg.reply-to, to-json(%resp);
        }
    }

    whenever $health-sub.supply -> $msg {
        if $msg.?reply-to {
            $nats.publish: $msg.reply-to, to-json({
                :status<ok>, :service<skill-store>,
                :backend<jetstream>, :stream<SKILLS>,
                :indexed(+%index),
            });
        }
    }
}
