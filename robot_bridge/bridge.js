require('dotenv').config();
// ── Global Error Resilience ──
process.on('uncaughtException', (err) => {
    console.error('>>> [CRITICAL] Uncaught Exception:', err.message);
    if (err.stack) console.error(err.stack);
});
process.on('unhandledRejection', (reason, p) => {
    console.error('>>> [CRITICAL] Unhandled Rejection at:', p, 'reason:', reason);
});

const { JSDOM } = require('jsdom');
const dom = new JSDOM('', {
    url: 'http://localhost',
    userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
});
const { window } = dom;

// --- Browser Globals for Node.js ---
global.window = window;
global.document = window.document;
global.navigator = window.navigator;
global.location = window.location;

const wrtc = require('@roamhq/wrtc');
const WS = require('ws');
global.WebSocket = WS;
window.WebSocket = WS; 

// Mirror WebRTC to window & global for PeerJS
const RTCPrototypes = [
    { name: 'RTCPeerConnection', val: wrtc.RTCPeerConnection },
    { name: 'RTCSessionDescription', val: wrtc.RTCSessionDescription },
    { name: 'RTCIceCandidate', val: wrtc.RTCIceCandidate },
    { name: 'MediaStream', val: wrtc.MediaStream }
];

RTCPrototypes.forEach(({ name, val }) => {
    window[name] = val;
    global[name] = val;
});

// ── Stability Shims for webrtc-adapter ──
wrtc.RTCIceCandidate.prototype.foundation = '';
wrtc.RTCIceCandidate.prototype.relayProtocol = '';
wrtc.RTCPeerConnection.prototype.connectionState = 'new';
wrtc.RTCPeerConnection.prototype.sctp = null;

const { Peer } = require('peerjs');
const WebSocket = require('ws');
const { createCanvas, Image } = require('canvas');

// --- Configuration ---
const ROBOT_PEER_ID = process.env.ROBOT_ID || 'robot1';
const ROBOT_PASSWORD = process.env.ROBOT_PASSWORD || ''; // Empty means NO AUTH REQUIRED (unsafe)
const IPC_PORT = process.env.IPC_PORT || 8765;

// --- State ---
let browserConn = null; // DataChannel connection
let isAuthenticated = false;
let authTimeout = null;
let activeCall = null;   // MediaStream call
let pythonWs = null;    // Internal ROS2 link

// ── WebRTC Media Source ───
// We create virtual video sources that we'll feed with decoded JPEGs
const topVideoSource = new wrtc.nonstandard.RTCVideoSource();
const topTrack = topVideoSource.createTrack();
const bottomVideoSource = new wrtc.nonstandard.RTCVideoSource();
const bottomTrack = bottomVideoSource.createTrack();

const stream = new MediaStream([topTrack, bottomTrack]);

// --- Camera 1 (TOP) Resources ---
const topCanvas = createCanvas(640, 480);
const topCtx = topCanvas.getContext('2d');
const topImg = new Image();

// --- Camera 2 (BOTTOM) Resources ---
const bottomCanvas = createCanvas(640, 480);
const bottomCtx = bottomCanvas.getContext('2d');
const bottomImg = new Image();

// ── Optimized Pixel Format Conversion ───
// Subsamples by 2 in both directions to save 75% CPU time.
// This scales $640 \times 480$ RGBA down to $320 \times 240$ I420.
function rgbaToI420(rgba, width, height) {
    const sw = width >> 1;  // subsampled width
    const sh = height >> 1; // subsampled height
    const ySize = sw * sh;
    const uvSize = ySize / 4;
    const i420 = new Uint8ClampedArray(ySize + 2 * uvSize);

    let yIdx = 0;
    let uIdx = ySize;
    let vIdx = ySize + uvSize;

    // We only iterate every 2nd pixel to save massive CPU
    for (let j = 0; j < height; j += 2) {
        for (let i = 0; i < width; i += 2) {
            const pixelIdx = (j * width + i) * 4;
            const r = rgba[pixelIdx];
            const g = rgba[pixelIdx + 1];
            const b = rgba[pixelIdx + 2];

            // Y component
            i420[yIdx++] = ((66 * r + 129 * g + 25 * b + 128) >> 8) + 16;

            // U and V (further subsampled)
            if (j % 4 === 0 && i % 4 === 0) {
                i420[uIdx++] = ((-38 * r - 74 * g + 112 * b + 128) >> 8) + 128;
                i420[vIdx++] = ((112 * r - 94 * g - 18 * b + 128) >> 8) + 128;
            }
        }
    }
    return { data: i420, width: sw, height: sh };
}

let lastFrameLog = 0;
function pushFrameToSource(source, base64Data, canvas, ctx, img) {
    // GATE: Only decode if there is an active call
    if (!activeCall) return;

    img.onload = () => {
        const start = Date.now();
        if (canvas.width !== img.width || canvas.height !== img.height) {
            canvas.width = img.width;
            canvas.height = img.height;
        }
        ctx.drawImage(img, 0, 0);
        const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
        
        // Convert RGBA -> I420 for WebRTC compatibility (Downsampled for speed)
        const res = rgbaToI420(imageData.data, canvas.width, canvas.height);
        
        source.onFrame({
            width: res.width,
            height: res.height,
            data: res.data
        });
        
        const end = Date.now();
        const now = Date.now();
        if (now - lastFrameLog > 5000) {
            console.log(`>>> [PEER] Frame Pushed. Conv Time: ${end - start}ms | Res: ${res.width}x${res.height}`);
            lastFrameLog = now;
        }
    };
    img.onerror = (err) => console.error('>>> [PEER] Image Decode Error:', err);
    img.src = 'data:image/jpeg;base64,' + base64Data;
}

// ═══════════════════════════════════════════════════════════════
// PEERJS SETUP (P2P Link)
// ═══════════════════════════════════════════════════════════════
const peer = new Peer(ROBOT_PEER_ID, {
    host: '0.peerjs.com',
    port: 443,
    secure: true,
    path: '/',
    debug: 2,
    config: {
        iceServers: [
            { urls: 'stun:stun.l.google.com:19302' },
            { urls: 'stun:stun1.l.google.com:19302' }
        ]
    }
});

peer.on('open', (id) => {
    console.log(`>>> [PEER] Robot Online. ID: ${id}`);
});

peer.on('connection', (conn) => {
    console.log(`>>> [PEER] Incoming connection request: ${conn.peer}`);
    
    // Graceful handover
    if (browserConn) {
        console.log('>>> [PEER] Replacing existing connection and call...');
        if (activeCall) { try { activeCall.close(); } catch(e) {} activeCall = null; }
        const old = browserConn;
        browserConn = null;
        try { old.close(); } catch(e) {}
    }
    
    browserConn = conn;
    isAuthenticated = false; // Reset on new connection

    // Close if not authenticated within 10s
    if (ROBOT_PASSWORD) {
        if (authTimeout) clearTimeout(authTimeout);
        authTimeout = setTimeout(() => {
            if (!isAuthenticated && browserConn === conn) {
                console.warn('>>> [AUTH] Handshake TIMEOUT. Closing rogue connection.');
                conn.send(JSON.stringify({ type: 'error', message: 'Auth Timeout' }));
                conn.close();
            }
        }, 10000);
    } else {
        // PERMISSIVE MODE: If no password set in .env, auto-auth
        isAuthenticated = true;
    }

    conn.on('data', (raw) => {
        try {
            const msg = typeof raw === 'string' ? JSON.parse(raw) : raw;
            
            // 1. Authentication Handshake
            if (msg.type === 'auth') {
                if (!ROBOT_PASSWORD || msg.password === ROBOT_PASSWORD) {
                    console.log('>>> [AUTH] Handshake SUCCESS. Unlocking system.');
                    isAuthenticated = true;
                    if (authTimeout) clearTimeout(authTimeout);
                    conn.send(JSON.stringify({ type: 'auth_ok' }));
                    
                    // Securely initiate MediaStream call ONLY after auth
                    console.log(`>>> [PEER] Initiating MediaStream call to: ${conn.peer}`);
                    activeCall = peer.call(conn.peer, stream);
                } else {
                    console.warn('>>> [AUTH] Handshake FAILED: Invalid Password');
                    conn.send(JSON.stringify({ type: 'auth_fail', reason: 'invalid_password' }));
                    setTimeout(() => conn.close(), 500);
                }
                return;
            }

            // 2. Security Gate
            if (!isAuthenticated) {
                console.warn('>>> [AUTH] Protocol Violated: Command received before Auth. Ignoring.');
                return;
            }

            // 3. Regular Command Flow (Ping/Pong and ROS2 commands)
            if (msg.type === 'ping') {
                conn.send(JSON.stringify({ type: 'pong', ts: msg.ts }));
                return;
            }

            if (pythonWs && pythonWs.readyState === WebSocket.OPEN) {
                pythonWs.send(JSON.stringify(msg));
            }
        } catch (e) {
            console.error('>>> [PEER] Invalid Message Format:', e.message);
        }
    });

    conn.on('close', () => {
        console.log(`>>> [PEER] Connection closed: ${conn.peer}`);
        if (activeCall) { try { activeCall.close(); } catch(e) {} activeCall = null; }
        if (browserConn === conn) {
            browserConn = null;
            isAuthenticated = false;
        }
        if (authTimeout) { clearTimeout(authTimeout); authTimeout = null; }
    });

    conn.on('error', (err) => {
        console.error('>>> [PEER] Connection Error:', err.message);
    });
});

peer.on('error', (err) => {
    if (err.type === 'network') {
        console.error('>>> [PEER] Network Error (DNS/Server Offline). Will retry...');
    } else {
        console.error('>>> [PEER] Global Error:', err.type, err.message);
    }
});

peer.on('disconnected', () => {
    console.warn('>>> [PEER] Disconnected from signaling server. Reconnecting...');
    if (!peer.destroyed) peer.reconnect();
});

// ═══════════════════════════════════════════════════════════════
// WEBSOCKET IPC SETUP (Local Link to Python)
// ═══════════════════════════════════════════════════════════════
const wss = new WebSocket.Server({ port: IPC_PORT });
console.log(`>>> [IPC] Internal Server Listening: ${IPC_PORT}`);

wss.on('connection', (ws) => {
    console.log('>>> [IPC] Python ROS2 Node Linked');
    pythonWs = ws;

    ws.on('message', (message) => {
        const raw = message.toString();

        // 1. MediaStream Relay (High performance)
        if (raw.includes('"camera_top"')) {
            try {
                const msg = JSON.parse(raw);
                if (!msg.data) { console.warn('>>> [IPC] empty camera_top data'); return; }
                pushFrameToSource(topVideoSource, msg.data, topCanvas, topCtx, topImg);
            } catch(e) { console.error('>>> [IPC] camera_top decode fail:', e.message); }
        } else if (raw.includes('"camera_bottom"')) {
            try {
                const msg = JSON.parse(raw);
                if (!msg.data) { console.warn('>>> [IPC] empty camera_bottom data'); return; }
                pushFrameToSource(bottomVideoSource, msg.data, bottomCanvas, bottomCtx, bottomImg);
            } catch(e) { console.error('>>> [IPC] camera_bottom decode fail:', e.message); }
        } else if (raw.includes('"battery"') || raw.includes('"robot_pose"')) {
            // Log telemetry presence occasionally
            if (Math.random() < 0.05) console.log('>>> [IPC] Telemetry relaying...');
        }

        // 2. DataChannel Relay (Backup & Telemetry)
        if (browserConn && browserConn.open) {
            const dc = browserConn.dataChannel;
            // Only send if the raw DataChannel is actually in the 'open' state
            if (!dc || dc.readyState !== 'open') return;

            if (dc.bufferedAmount > 256 * 1024) {
                if (raw.includes('"camera_top"') || raw.includes('"camera_bottom"') || raw.includes('"robot_pose"')) {
                    return; 
                }
            }
            try {
                browserConn.send(raw);
            } catch(e) { }
        }
    });

    ws.on('close', () => {
        console.log('>>> [IPC] Python ROS2 Node Disconnected');
        pythonWs = null;
    });

    ws.on('error', (err) => {
        console.error('>>> [IPC] WebSocket Error:', err.message);
    });
});

// ═══════════════════════════════════════════════════════════════
// PROCESS LOGIC
// ═══════════════════════════════════════════════════════════════
process.on('SIGINT', () => {
    console.log('>>> [BRIDGE] Shutting down...');
    if (activeCall) activeCall.close();
    if (browserConn) browserConn.close();
    peer.destroy();
    process.exit();
});
