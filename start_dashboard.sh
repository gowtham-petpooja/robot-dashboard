#!/bin/bash

# --- ROBOT DASHBOARD UNIFIED STARTUP ---
# This script launches the signaling server, P2P bridge, and ROS2 link.

# Get the script's directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Kill existing processes on port 9000, 8765, and 8080 to avoid EADDRINUSE
echo ">>> [SETUP] Cleaning up old sessions..."
fuser -k 9000/tcp 2>/dev/null
fuser -k 8765/tcp 2>/dev/null
fuser -k 8080/tcp 2>/dev/null

# Handler for graceful shutdown
cleanup() {
    echo -e "\n>>> [SHUTDOWN] Terminating all components..."
    kill $PEER_PID $BRIDGE_PID $ROS_PID $HTTP_PID 2>/dev/null
    exit
}
trap cleanup SIGINT SIGTERM

echo ">>> [1/4] Starting Private PeerJS Server (Port 9000)..."
peerjs --port 9000 --path / > "$DIR/peerjs.log" 2>&1 &
PEER_PID=$!
sleep 2

echo ">>> [2/4] Starting Node.js P2P Bridge..."
cd "$DIR/robot_bridge"
node bridge.js > "$DIR/bridge.log" 2>&1 &
BRIDGE_PID=$!
sleep 2

echo ">>> [3/4] Starting ROS2 P2P Bridge..."
cd "$DIR"
python3 ros_p2p_bridge.py > "$DIR/ros.log" 2>&1 &
ROS_PID=$!
sleep 2

echo ">>> [4/4] Starting Static Web Server (Port 8080)..."
python3 -m http.server 8080 > "$DIR/http.log" 2>&1 &
HTTP_PID=$!

echo "===================================================="
echo "✨ ROBOT CORE MONITOR IS ONLINE!"
echo "===================================================="
echo "🔗 Dashboard:  http://localhost:8080 (or your Tailscale IP)"
echo "📡 Signaling: Port 9000"
echo "🤖 Robot ID:  $(grep ROBOT_ID "$DIR/robot_bridge/.env" | cut -d'=' -f2)"
echo "----------------------------------------------------"
echo "Logs are available in: bridge.log, ros.log, peerjs.log"
echo "Press Ctrl+C to shut down all components."
echo "===================================================="

# Keep script running to maintain child processes
wait
