#!/bin/bash
# setup.sh — One-time setup for Polymarket v2 data collector
# Run this ONCE after cloning / before first launch.
# Usage: bash setup.sh

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "🔧 Setting up Polymarket v2 collector at: $PROJECT_DIR"

# --- 1. Archive old core files ---
echo "📦 Archiving old core files..."
mkdir -p "$PROJECT_DIR/Old-content/core"
for f in core/ingestor.py core/monitor.py core/expand_markets.py core/discovery.py; do
    [ -f "$PROJECT_DIR/$f" ] && cp "$PROJECT_DIR/$f" "$PROJECT_DIR/Old-content/core/" && echo "  Archived: $f"
done
[ -f "$PROJECT_DIR/run_monitor.py" ] && cp "$PROJECT_DIR/run_monitor.py" "$PROJECT_DIR/Old-content/" && echo "  Archived: run_monitor.py"

# --- 2. Remove dead scripts ---
echo "🗑  Removing old scripts..."
for f in main.py ingest_all_events.py core/discovery.py; do
    [ -f "$PROJECT_DIR/$f" ] && rm "$PROJECT_DIR/$f" && echo "  Deleted: $f"
done

# --- 3. Create logs directory ---
mkdir -p "$PROJECT_DIR/logs"
echo "📁 Created logs/ directory"

# --- 4. Install dependencies ---
echo "📥 Installing Python dependencies..."
if [ -d "$PROJECT_DIR/venv" ]; then
    source "$PROJECT_DIR/venv/bin/activate"
else
    python3 -m venv "$PROJECT_DIR/venv"
    source "$PROJECT_DIR/venv/bin/activate"
fi
pip install -q --upgrade pip
pip install -q -r "$PROJECT_DIR/requirements.txt"

# --- 5. Install launchd background service (macOS) ---
PLIST_SRC="$PROJECT_DIR/polymarket.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.polymarket.collector.plist"

if [ -f "$PLIST_SRC" ]; then
    echo "⚙️  Installing launchd agent..."
    # Inject the actual Python path and project dir into the plist
    PYTHON_PATH="$(which python3)"
    sed -e "s|__PROJECT_DIR__|$PROJECT_DIR|g" \
        -e "s|__PYTHON_PATH__|$PROJECT_DIR/venv/bin/python|g" \
        "$PLIST_SRC" > "$PLIST_DST"

    launchctl unload "$PLIST_DST" 2>/dev/null || true
    launchctl load "$PLIST_DST"
    echo "✅ Background service installed and started!"
    echo "   To check status:  launchctl list | grep polymarket"
    echo "   To view logs:     tail -f $PROJECT_DIR/logs/collector.log"
    echo "   To stop:          launchctl unload $PLIST_DST"
    echo "   To start:         launchctl load $PLIST_DST"
else
    echo "⚠️  polymarket.plist not found — skipping launchd install"
    echo "   Run manually with: python run_collector.py"
fi

echo ""
echo "✅ Setup complete! The collector should now be running in the background."
