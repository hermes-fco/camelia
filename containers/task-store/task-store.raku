#!/usr/bin/env raku
# 🌺 Camélia — Task Store (core NATS + Channel, no react)
#
# SQLite-backed task queue. Channel pattern avoids nats.raku multi-sub bug.

use Nats;
use JSON::Fast;

$*ERR.out-buffer = False;
my $nats-url = %*ENV<NATS_URL> // 'nats://127.0.0.1:4222';
my $db-path  = %*ENV<DB_PATH>  // '/data/tasks.db';

note "🟡 Task Store connecting NATS ($nats-url)...";
my $nats = Nats.new: :servers[$nats-url];
await $nats.start; $nats.connect;
note "🟢 Task Store connected.";

# SQLite helpers
sub sql-exec(Str $sql --> Bool) {
    my $proc = Proc::Async.new('sqlite3', $db-path, $sql);
    my $err = ''; $proc.stderr.lines(:chomp).tap(-> $line { $err ~= $line });
    my $r = await $proc.start;
    if $r.exitcode != 0 { note "  ⚠️ SQL error: $err"; return False }
    True
}
sub sql-query(Str $sql --> List) {
    my $proc = Proc::Async.new('sqlite3', '-json', $db-path, $sql);
    my $out = ''; my $err = '';
    $proc.stdout.lines(:chomp).tap(-> $line { $out ~= $line });
    $proc.stderr.lines(:chomp).tap(-> $line { $err ~= $line });
    my $r = await $proc.start;
    if $r.exitcode != 0 { note "  ⚠️ SQL error: $err"; return [] }
    return [] unless $out.trim;
    my $parsed = try from-json($out);
    return [] if $! || !$parsed;
    $parsed ~~ Array ?? $parsed.List !! [$parsed].List;
}

note "📦 Initializing SQLite at $db-path";
sql-exec(q:to/SCHEMA/);
    CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY, description TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending'
            CHECK(status IN ('pending','assigned','in_progress','completed','failed','cancelled')),
        priority INTEGER DEFAULT 0, created_by TEXT DEFAULT '',
        assigned_to TEXT DEFAULT '', worker_type TEXT DEFAULT '',
        session_id TEXT DEFAULT '', chat_id TEXT DEFAULT '',
        result TEXT DEFAULT '', error_msg TEXT DEFAULT '',
        attempts INTEGER DEFAULT 0, max_attempts INTEGER DEFAULT 3,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        started_at TEXT DEFAULT '', completed_at TEXT DEFAULT '',
        scheduled_at TEXT DEFAULT ''
    );
    CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
    CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority, created_at);
    SCHEMA
note "✅ Database ready.";

sub gen-id { "tsk-" ~ (('a'..'z').pick xx 12).join }
sub now-str { DateTime.now.utc.DateTime.Str.substr(0, 19) ~ 'Z' }
sub reply(Str $to, %d) { $nats.publish: $to, to-json(%d, :!pretty) }
sub esc(Str $s) { $s.subst("'", "''", :g) }

# ═══ Handlers (unchanged) ═══

sub handle-create(Str $reply-to, %req) {
    my $desc = %req<description> // '';
    unless $desc { reply($reply-to, { :error("Missing description") }); return }
    my $id = %req<id> // gen-id(); my $now = now-str();
    sql-exec("INSERT INTO tasks (id, description, priority, created_by, worker_type,
        session_id, chat_id, max_attempts, created_at, updated_at)
        VALUES ('{esc($id)}', '{esc($desc)}', {%req<priority> // 0},
        '{esc(%req<created_by> // '')}', '{esc(%req<worker_type> // '')}',
        '{esc(%req<session_id> // '')}', '{esc(%req<chat_id> // '')}',
        {%req<max_attempts> // 3}, '{$now}', '{$now}')");
    note "  📝 Created task {$id}";
    reply($reply-to, { :ok(True), :$id, :status<pending> });
}

sub handle-update(Str $reply-to, %req) {
    my $id = %req<id> // '';
    unless $id { reply($reply-to, { :error("Missing id") }); return }
    my @rows = sql-query("SELECT status FROM tasks WHERE id = '{esc($id)}'");
    unless @rows { reply($reply-to, { :error("Task not found: $id") }); return }
    my $cur = @rows[0]<status>; my $new = %req<status> // $cur; my $now = now-str();
    my @sets = "status = '{esc($new)}'", "updated_at = '{$now}'";
    if %req<result>:exists    { @sets.push: "result = '{esc(%req<result>)}'" }
    if %req<error_msg>:exists { @sets.push: "error_msg = '{esc(%req<error_msg>)}'" }
    if %req<assigned_to>:exists { @sets.push: "assigned_to = '{esc(%req<assigned_to>)}'" }
    if $new eq 'in_progress' && $cur ne 'in_progress' { @sets.push: "attempts = attempts + 1"; @sets.push: "started_at = '{$now}'" }
    if $new eq 'completed'|'failed'|'cancelled' { @sets.push: "completed_at = '{$now}'" }
    sql-exec("UPDATE tasks SET {@sets.join(', ')} WHERE id = '{esc($id)}'");
    note "  🔄 Task {$id}: {$cur} → {$new}";
    reply($reply-to, { :ok(True), :$id, :status($new) });
}

sub handle-get(Str $reply-to, %req) {
    my $id = %req<id> // '';
    unless $id { reply($reply-to, { :error("Missing id") }); return }
    my @rows = sql-query("SELECT * FROM tasks WHERE id = '{esc($id)}'");
    unless @rows { reply($reply-to, { :error("Task not found: $id") }); return }
    reply($reply-to, { :ok(True), :task(@rows[0]) });
}

sub handle-list(Str $reply-to, %req) {
    my $status = %req<status> // ''; my $limit = %req<limit> // 50; my $offset = %req<offset> // 0;
    my $where = $status ?? "WHERE status = '{esc($status)}'" !! '';
    my @count-r = sql-query("SELECT COUNT(*) as cnt FROM tasks {$where}");
    my $count = @count-r ?? (@count-r[0]<cnt> // 0) !! 0;
    my @tasks = sql-query("SELECT * FROM tasks {$where} ORDER BY priority DESC, created_at ASC LIMIT {$limit} OFFSET {$offset}");
    reply($reply-to, { :ok(True), :$count, :tasks(@tasks) });
}

sub handle-delete(Str $reply-to, %req) {
    my $id = %req<id> // '';
    unless $id { reply($reply-to, { :error("Missing id") }); return }
    sql-exec("DELETE FROM tasks WHERE id = '{esc($id)}'");
    note "  🗑️ Deleted task {$id}";
    reply($reply-to, { :ok(True), :deleted($id) });
}

sub handle-next(Str $reply-to, %req) {
    my $wtype = %req<worker_type> // '';
    sql-exec('BEGIN IMMEDIATE');
    my $where = "status = 'pending'";
    if $wtype { $where ~= " AND (worker_type = '{esc($wtype)}' OR worker_type = '')" }
    my @rows = sql-query("SELECT id FROM tasks WHERE {$where} ORDER BY priority DESC, created_at ASC LIMIT 1");
    unless @rows { sql-exec('ROLLBACK'); reply($reply-to, { :ok(True), :task(Nil), :message("No pending tasks") }); return }
    my $id = @rows[0]<id>; my $now = now-str();
    sql-exec("UPDATE tasks SET status = 'assigned', updated_at = '{$now}', started_at = '{$now}', attempts = attempts + 1 WHERE id = '{esc($id)}'");
    sql-exec('COMMIT');
    my @task = sql-query("SELECT * FROM tasks WHERE id = '{esc($id)}'");
    note "  🎯 Claimed task {$id}";
    reply($reply-to, { :ok(True), :task(@task[0]) });
}

# ═══ REACT LOOP ═══
my $sub = $nats.subscribe: 'task.store.>';
note "🔄 Task Store ready.";

my $last-alive = now.Int;
react {
    whenever $sub.supply -> $msg {
        # ✅ try protege contra JSON inválido
        my $parsed = try from-json($msg.payload);
        unless $parsed.defined {
            note "⚠️ JSON inválido em {$msg.subject}, ignorando";
            next;
        }
        my $reply-to = $msg.?reply-to;
        unless $reply-to { note "⚠️ No reply-to on {$msg.subject}"; next }

        my %req = $parsed;

        given $msg.subject {
            when /create$/ { handle-create($reply-to, %req) }
            when /update$/ { handle-update($reply-to, %req) }
            when /get$/    { handle-get($reply-to, %req) }
            when /list$/   { handle-list($reply-to, %req) }
            when /delete$/ { handle-delete($reply-to, %req) }
            when /next$/   { handle-next($reply-to, %req) }
            when /health/  { reply($reply-to, { :status<ok>, :service<task-store> }) }
        }

        my $now = now.Int;
        if $now - $last-alive > 300 { note "💚 Task Store alive"; $last-alive = $now }
    }
}
