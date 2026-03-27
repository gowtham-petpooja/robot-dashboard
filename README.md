# Robot Dashboard - P2P WebRTC Core

A decentralised, high-performance robot dashboard using PeerJS (WebRTC) for direct browser-to-robot telemetry and control.

## Key Features
- **P2P Architecture**: No central server; direct encrypted DataChannels between operator and robot.
- **Static Frontend**: Fully compatible with GitHub Pages or any static host.
- **ROS 2 Integration**: Includes a Node.js bridge to relay ROS 2 topics over WebRTC.
- **Rich Telemetry**: Real-time support for Map, Path, Scan, Battery, and dual Image feeds.

## Quick Start

### 1. Unified Startup
From the root of the project, run:
```bash
./startup.sh
```
This script will launch both the Node.js signaling bridge and the Python ROS 2 bridge. It handles environment sourcing and process cleanup automatically.

### 2. Operator Dashboard
Open `index.html` in a modern browser.
Enter the Robot Peer ID (default: `robot-petpooja-1`) and click **Establish Link**.

## Directory Structure
- `index.html`: Optimized static operator dashboard.
- `robot_bridge/`: Node.js signaling and WebRTC shim for server-side operation.
- `ros_p2p_bridge.py`: Python-based ROS 2 topic aggregator and IPC client.