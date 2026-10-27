#!/bin/sh
# Reports how far this repository's modules have drifted from the market
# maker they were extracted from. It copies nothing: the modules here were
# adapted (self-contained docs, neutral defaults, no strategy hooks), so a
# blind copy would undo that. Read the diff, port what matters, by hand.
#
#   tools/drift.sh [path to the polybot-rs crate]   (default: ../polybot/polybot-rs)
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
src=${1:-"$here/../polybot/polybot-rs"}
if [ ! -d "$src/src" ]; then
    echo "no crate at $src" >&2
    exit 2
fi
echo "source: $src at $(git -C "$src" rev-parse --short HEAD 2>/dev/null || echo '(not a git checkout)')"
printf '%-36s %-36s %8s %8s\n' "public" "source" "+lines" "-lines"
total=0
while read -r pub priv; do
    if [ ! -f "$src/$priv" ]; then
        printf '%-36s %-36s %17s\n' "$pub" "$priv" "missing"
        continue
    fi
    added=$(diff -U0 "$src/$priv" "$here/$pub" | grep -c '^+[^+]' || true)
    removed=$(diff -U0 "$src/$priv" "$here/$pub" | grep -c '^-[^-]' || true)
    total=$((total + added + removed))
    printf '%-36s %-36s %8s %8s\n' "$pub" "$priv" "$added" "$removed"
done <<LIST
src/abi.rs src/abi.rs
src/accounting.rs src/accounting.rs
src/execution.rs src/execution.rs
src/fees.rs src/fees.rs
src/market_data.rs src/market_data.rs
src/receipts.rs src/receipts.rs
src/polymarket/auth.rs src/polymarket/auth.rs
src/polymarket/signing.rs src/polymarket/signing.rs
src/ws/feed_lag.rs src/ws/feed_lag.rs
src/ws/feed_merge.rs src/ws/feed_merge.rs
src/ws/fill_stream.rs src/ws/fill_tracker.rs
examples/order_latency_probe.rs src/bin/order_latency_probe.rs
LIST
echo "total differing lines: $total"
