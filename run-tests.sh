#!/bin/bash
# 🌺 Camélia — Run all tests (unit + integration)
# Usage: ./run-tests.sh

set -e
NATS_LIB="/root/forks/nats.raku/lib"
RAKU=/root/bin/raku

echo "═══════════════════════════════════════════"
echo "  nats.raku — Unit Tests"
echo "═══════════════════════════════════════════"
FAIL=0
for t in /root/forks/nats.raku/t/*.rakutest; do
    name=$(basename "$t")
    if $RAKU -I "$NATS_LIB" "$t" 2>&1 | grep -q "^not ok"; then
        echo "  ❌ $name"
        FAIL=1
    else
        echo "  ✅ $name"
    fi
done

echo ""
echo "═══════════════════════════════════════════"
echo "  nats.raku — Integration Tests"
echo "═══════════════════════════════════════════"
$RAKU -I "$NATS_LIB" /root/forks/nats.raku/t/integration.rakutest 2>&1 | grep -E "^(ok|not ok|# Subtest|# You)"
INT_EXIT=${PIPESTATUS[0]}
if [ $INT_EXIT -ne 0 ]; then
    FAIL=1
fi

echo ""
echo "═══════════════════════════════════════════"
echo "  Camélia — Integration Tests"
echo "═══════════════════════════════════════════"
CT="camelia-test-$$"
docker rm -f "$CT" 2>/dev/null || true
docker create --name "$CT" --network camelia-net \
    --entrypoint tail camelia-worker:latest -f /dev/null > /dev/null 2>&1
docker cp /root/camelia/tests/camelia-integration.raku "$CT":/tmp/test.raku > /dev/null 2>&1
docker cp "$NATS_LIB" "$CT":/tmp/nats-lib > /dev/null 2>&1
docker start "$CT" > /dev/null 2>&1
docker exec "$CT" raku -I /tmp/nats-lib /tmp/test.raku 2>&1
CAM_EXIT=$?
docker rm -f "$CT" > /dev/null 2>&1
if [ $CAM_EXIT -ne 0 ]; then
    FAIL=1
fi

echo ""
if [ $FAIL -eq 0 ]; then
    echo "✅ All test suites passed!"
else
    echo "❌ Some tests failed"
fi
exit $FAIL
