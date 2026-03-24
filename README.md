# Robot Dashboard - P2P WebRTC Core

A decentralised, high-performance robot dashboard using PeerJS (WebRTC) for direct browser-to-robot telemetry and control.

## Key Features
- **P2P Architecture**: No central server; direct encrypted DataChannels between operator and robot.
- **Static Frontend**: Fully compatible with GitHub Pages or any static host.
- **ROS 2 Integration**: Includes a Node.js bridge to relay ROS 2 topics over WebRTC.
- **Rich Telemetry**: Real-time support for Map, Path, Scan, Battery, and dual Image feeds.

## Quick Start

### 1. Robot-Side Setup
Navigate to the robot bridge and start the signaling node:
```bash
cd robot_bridge
node bridge.js
```

### 2. ROS 2 Bridge
In a separate terminal with your ROS 2 environment sourced:
```bash
python3 ros_p2p_bridge.py
```

### 3. Operator Dashboard
Open `index.html` in a modern browser.
Enter the Robot Peer ID (default: `robot-petpooja-1`) and click **Establish Link**.

## Directory Structure
- `index.html`: Optimized static operator dashboard.
- `robot_bridge/`: Node.js signaling and WebRTC shim for server-side operation.
- `ros_p2p_bridge.py`: Python-based ROS 2 topic aggregator and IPC client.