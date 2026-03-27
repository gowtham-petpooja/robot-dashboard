#!/bin/bash

# --- Configuration ---
ROS_DISTRO_PATH="/opt/ros/humble/setup.bash"
DASHBOARD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Initialization ---
echo "------------------------------------------------"
echo "  Robot Dashboard Unified Startup Script"
echo "------------------------------------------------"

# Source ROS 2 environment if available
if [ -f "$ROS_DISTRO_PATH" ]; then
    echo "[INFO] Sourcing ROS 2 ($ROS_DISTRO_PATH)..."
    source "$ROS_DISTRO_PATH"
else
    echo "[WARN] ROS 2 setup file not found at $ROS_DISTRO_PATH"
fi

# --- Cleanup Logic ---
cleanup() {
    echo ""
    echo "[SHUTDOWN] Terminating background processes..."
    # Kill all background jobs started by this script
    kill $(jobs -p) 2>/dev/null
    wait
    echo "[SHUTDOWN] All processes stopped."
    exit 0
}

# Trap Ctrl+C and termination signals
trap cleanup SIGINT SIGTERM

# --- Launch Services ---
cd "$DASHBOARD_DIR"

echo "[LAUNCH] Starting Node.js Bridge..."
(cd robot_bridge && node bridge.js) &
NODE_PID=$!

# Give the signaling server a moment to bind
sleep 2

echo "[LAUNCH] Starting Python ROS 2 Bridge..."
python3 ros_p2p_bridge.py &
PYTHON_PID=$!

echo "[READY] Services are running. Press [Ctrl+C] to stop."
echo "------------------------------------------------"

# Keep the script running and follow background logs
wait
