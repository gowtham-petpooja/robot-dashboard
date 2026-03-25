import rclpy
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
from sensor_msgs.msg import CompressedImage, LaserScan
from nav_msgs.msg import OccupancyGrid, Path
from std_msgs.msg import String, Float32, Int32
from geometry_msgs.msg import Twist, PoseWithCovarianceStamped, Quaternion, PoseStamped
from rcl_interfaces.msg import Log
import tf2_ros
import base64
import cv2
import numpy as np
import math
import sys
import subprocess
import os
import signal
import csv
import json
import asyncio
import websockets
import threading
import time

# ── Configuration ──────────────────────────────────────────────────────────
NODE_BRIDGE_URL = "ws://localhost:8765"

# ── QoS Profiles ──────────────────────────────────────────────────────────

_sensor_qos = QoSProfile(
    reliability=ReliabilityPolicy.BEST_EFFORT,
    history=HistoryPolicy.KEEP_LAST,
    depth=1
)

_map_qos = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    durability=DurabilityPolicy.TRANSIENT_LOCAL,
    history=HistoryPolicy.KEEP_LAST,
    depth=1
)

_log_qos = QoSProfile(
    depth=100
)

_volatile_map_qos = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    durability=DurabilityPolicy.VOLATILE,
    history=HistoryPolicy.KEEP_LAST,
    depth=1
)

# ── Async WebSocket Bridge ──────────────────────────────────────────────────
class AsyncBridge:
    def __init__(self):
        self.ws = None
        self.loop = asyncio.new_event_loop()
        
        self.latest_volatile = {}
        self.ordered_queue = asyncio.Queue()
        self.data_ready = asyncio.Event()
        
        self.on_message_callback = None
        self._running = True
        
        self.volatile_types = {
            'camera_top', 'camera_bottom', 'robot_pose', 'scan_data',
            'battery', 'path_data', 'nav2_status'
            # costmap types removed
        }

    def set_on_message(self, callback):
        self.on_message_callback = callback

    async def _connect_and_listen(self):
        while self._running:
            try:
                print(f">>> [IPC] Connecting to Node Bridge at {NODE_BRIDGE_URL}...")
                async with websockets.connect(NODE_BRIDGE_URL, ping_interval=1, ping_timeout=1) as websocket:
                    print(">>> [IPC] Connected to Node Bridge")
                    self.ws = websocket
                    self.data_ready.set()
                    
                    send_task = asyncio.create_task(self._send_loop())
                    recv_task = asyncio.create_task(self._recv_loop())
                    
                    done, pending = await asyncio.wait(
                        [send_task, recv_task],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in pending:
                        task.cancel()
                    
            except Exception as e:
                print(f"ERROR: [IPC] Connection failed ({e}). Retrying in 3s...")
                self.ws = None
                await asyncio.sleep(3)

    async def _send_loop(self):
        while self.ws:
            await self.data_ready.wait()
            self.data_ready.clear()
            
            while not self.ordered_queue.empty():
                try:
                    msg = self.ordered_queue.get_nowait()
                    await asyncio.wait_for(self.ws.send(msg), timeout=0.5)
                except: break
            
            current_volatile = self.latest_volatile.copy()
            self.latest_volatile.clear()
            
            for mtype in self.volatile_types:
                if mtype in current_volatile:
                    try:
                        await asyncio.wait_for(self.ws.send(current_volatile[mtype]), timeout=0.5)
                    except: break

    async def _recv_loop(self):
        while self.ws:
            try:
                raw = await self.ws.recv()
                if self.on_message_callback:
                    self.on_message_callback(json.loads(raw))
            except:
                break

    def send(self, type, payload):
        if not self._running: return
        msg = json.dumps({"type": type, **payload})
        
        if type in self.volatile_types:
            self.latest_volatile[type] = msg
        else:
            self.loop.call_soon_threadsafe(self.ordered_queue.put_nowait, msg)
            
        self.loop.call_soon_threadsafe(self.data_ready.set)

    def start(self):
        threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._connect_and_listen())

    def stop(self):
        self._running = False
        self.loop.stop()

bridge = AsyncBridge()

# ── ROS2 Subscribers ────────────────────────────────────────────────────────────

class ImageSubscriber(Node):
    def __init__(self, node_name, topic_name, event_name, fps_limit=10):
        super().__init__(node_name)
        self.subscription = self.create_subscription(CompressedImage, topic_name, self.listener_callback, _sensor_qos)
        self.event_name = event_name
        self.latest_frame = None
        self._fps_limit = fps_limit
        self._running = True
        self._thread = threading.Thread(target=self.emit_loop, daemon=True)
        self._thread.start()

    def listener_callback(self, msg):
        # Store raw bytes directly — no copy overhead
        self.latest_frame = bytes(msg.data)

    def emit_loop(self):
        interval = 1.0 / self._fps_limit
        while self._running:
            start_time = time.monotonic()
            frame = self.latest_frame
            if frame:
                self.latest_frame = None
                try:
                    frame_encoded = base64.b64encode(frame).decode('utf-8')
                    bridge.send(self.event_name, {"data": frame_encoded})
                except: pass
            
            elapsed = time.monotonic() - start_time
            time.sleep(max(0, interval - elapsed))


class BatterySubscriber(Node):
    def __init__(self):
        super().__init__('web_battery_sub')
        self.percentage = 0.0
        self.is_charging = False
        self.subscription = self.create_subscription(Float32, '/robot2/battery_percentage', self.listener_callback, 10)
        self.charging_subscription = self.create_subscription(Int32, '/robot2/batt_ind', self.charging_callback, 10)

    def listener_callback(self, msg):
        self.percentage = round(float(msg.data), 1)
        self.emit_status()

    def charging_callback(self, msg):
        self.is_charging = (msg.data == 0)
        self.emit_status()

    def emit_status(self):
        bridge.send('battery', {
            'percentage': self.percentage,
            'charging': self.is_charging,
            'unplug_required': (self.percentage >= 100.0 and self.is_charging)
        })


class LogSubscriber(Node):
    def __init__(self):
        super().__init__('web_log_sub')
        self.levels = {10: "DEBUG", 20: "INFO", 30: "WARN", 40: "ERROR", 50: "FATAL"}
        self.subscription = self.create_subscription(Log, '/rosout', self.listener_callback, _log_qos)

    def listener_callback(self, msg):
        if "Nav2 Health" in msg.msg or "lifecycle_manager_navigation" in msg.msg:
            status = "OK" if "active" in msg.msg.lower() or "Health" in msg.msg else "ERROR"
            bridge.send('nav2_status', {'status': status, 'msg': msg.msg})

        bridge.send('ros_log', {
            'time': time.strftime("%H:%M:%S", time.localtime()),
            'name': msg.name,
            'level': self.levels.get(msg.level, "UNKNOWN"),
            'msg': msg.msg
        })


class NavDataSubscriber(Node):
    def __init__(self, tf_buffer):
        super().__init__('web_nav_sub')
        self.tf_buffer = tf_buffer
        self.last_map = None
        self.latest_scan = None
        self._map_dirty = False

        # ── Subscriptions (costmaps removed) ──
        self.map_sub = self.create_subscription(OccupancyGrid, '/robot2/map', self.map_callback, _map_qos)
        self.plan_sub = self.create_subscription(Path, '/robot2/plan', self.plan_callback, 10)
        self.scan_sub = self.create_subscription(LaserScan, '/robot2/scan', self.scan_callback, _sensor_qos)
        self.map_name_sub = self.create_subscription(String, '/robot2/map_name', self.map_name_callback, _volatile_map_qos)

        # ── Publishers ──
        self.initial_pose_pub = self.create_publisher(PoseWithCovarianceStamped, '/robot2/initialpose', 10)
        self.goal_pub = self.create_publisher(PoseStamped, '/robot2/goal_pose', 10)

        self.current_map_name = "New_Map"

        # Pose at 10 Hz, scan at 5 Hz — two separate timers
        self.pose_timer = self.create_timer(0.1,       self.publish_pose)
        self.scan_timer = self.create_timer(1.0 / 5.0, self.emit_latest_scan)

    # ── Map ──────────────────────────────────────────────────────────────

    def process_map(self, msg):
        """Convert OccupancyGrid → JPEG base64 and return adjusted metadata."""
        width, height = msg.info.width, msg.info.height
        resolution = float(msg.info.resolution)
        if width == 0 or height == 0:
            return None, 0, 0, 0.0
        data = np.array(msg.data, dtype=np.int8).reshape((height, width))

        # Build a 3-channel BGR image
        img = np.full((height, width, 3), 180, dtype=np.uint8)
        img[data == 0]   = [215, 215, 215]
        img[data == 100] = [45,  45,  45]

        img = cv2.flip(img, 0)

        # Downsample if map is large
        h, w = img.shape[:2]
        new_w, new_h = w, h
        new_res = resolution
        if h > 600 or w > 600:
            scale = 600.0 / max(h, w)
            new_w, new_h = int(w * scale), int(h * scale)
            new_res = resolution / scale
            img = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_NEAREST)

        _, buffer = cv2.imencode('.jpg', img, [cv2.IMWRITE_JPEG_QUALITY, 90])
        return base64.b64encode(buffer).decode('utf-8'), new_w, new_h, new_res

    def map_callback(self, msg):
        now = time.time()
        # Throttle: re-encode map at most every 3 seconds
        if hasattr(self, '_last_map_emit_time') and (now - self._last_map_emit_time < 3.0):
            return

        img_b64, w, h, res = self.process_map(msg)
        if img_b64:
            self.last_map = {
                'image':    img_b64,
                'resolution': res,
                'width':    w,
                'height':   h,
                'origin_x': float(msg.info.origin.position.x),
                'origin_y': float(msg.info.origin.position.y)
            }
            self._last_map_emit_time = now
            bridge.send('map_data', self.last_map)

    def emit_last_map(self):
        if self.last_map:
            bridge.send('map_data', self.last_map)
        self.load_map_goals(self.current_map_name)

    def map_name_callback(self, msg):
        new_name = msg.data.strip()
        if new_name != self.current_map_name:
            self.current_map_name = new_name
            self.load_map_goals(self.current_map_name)

    def load_map_goals(self, map_name):
        try:
            csv_path = f"/home/robot1/foodbot_ws/home_maps/{map_name}_goals.csv"
            goals = []
            if os.path.exists(csv_path):
                with open(csv_path, mode='r') as f:
                    for row in csv.DictReader(f):
                        goals.append({
                            'name': row['goal_name'],
                            'x': float(row['x']), 'y': float(row['y']),
                            'z': float(row['z']), 'w': float(row['w'])
                        })
                bridge.send('map_goals', {'map': map_name, 'goals': goals})
            else:
                bridge.send('map_goals', {'map': map_name, 'goals': [], 'error': 'FILE_NOT_FOUND'})
        except Exception as e:
            print(f"ERROR: Failed to load goals for {map_name}: {e}")

    # ── Scan ─────────────────────────────────────────────────────────────

    def scan_callback(self, msg):
        self.latest_scan = msg  # latest-wins, emitted by timer

    def emit_latest_scan(self):
        msg = self.latest_scan
        if not msg:
            return
        self.latest_scan = None

        ranges = msg.ranges
        amin   = msg.angle_min
        ainc   = msg.angle_increment
        rmin   = msg.range_min
        rmax   = msg.range_max

        points = []
        # Subsample every 8th ray (matches original) — keeps payload small
        for i in range(0, len(ranges), 8):
            r = ranges[i]
            if rmin < r < rmax:
                angle = amin + i * ainc + math.pi
                points.append({'x': float(r * math.cos(angle)), 'y': float(r * math.sin(angle))})
        bridge.send('scan_data', {'points': points})

    # ── Path ─────────────────────────────────────────────────────────────

    def plan_callback(self, msg):
        # Subsample path — no need to send every pose to the dashboard
        poses = msg.poses
        step  = max(1, len(poses) // 100)   # at most 100 points
        bridge.send('path_data', {
            'path': [{'x': float(p.pose.position.x), 'y': float(p.pose.position.y)}
                     for p in poses[::step]]
        })

    # ── Pose (TF) ─────────────────────────────────────────────────────────

    def publish_pose(self):
        if not self.tf_buffer:
            return
        try:
            try:
                trans = self.tf_buffer.lookup_transform('map', 'robot2/base_link', rclpy.time.Time())
            except:
                trans = self.tf_buffer.lookup_transform('map', 'base_link', rclpy.time.Time())

            q   = trans.transform.rotation
            yaw = float(math.atan2(2 * (q.w * q.z + q.x * q.y), 1 - 2 * (q.y * q.y + q.z * q.z)))
            bridge.send('robot_pose', {
                'x':     float(trans.transform.translation.x),
                'y':     float(trans.transform.translation.y),
                'yaw':   yaw,
                'stamp': trans.header.stamp.sec + trans.header.stamp.nanosec * 1e-9
            })
        except Exception as e:
            if hasattr(self, '_last_tf_err_time') and (time.time() - self._last_tf_err_time < 5.0):
                return
            print(f">>> [DEBUG] Pose lookup fail: {e}")
            self._last_tf_err_time = time.time()


# ── Process Command Logic ───────────────────────────────────────────
teleop_pub     = None
nav_node       = None
terminal_process = None

def on_bridge_message(msg):
    global teleop_pub, nav_node, terminal_process
    mtype = msg.get('type')

    if mtype == 'emergency_stop':
        if teleop_pub:
            pub_twist(0.0, 0.0)
            print(">>> [SAFETY] EMERGENCY STOP TRIGGERED")

    elif mtype == 'cmd_vel':
        cmd = msg.get('command')
        if cmd and teleop_pub:
            t = Twist()
            speed = 0.3 if cmd in ['W', 'S'] else 0.6
            if   cmd == 'W': t.linear.x  =  speed
            elif cmd == 'S': t.linear.x  = -speed
            elif cmd == 'A': t.angular.z =  speed
            elif cmd == 'D': t.angular.z = -speed
            teleop_pub.publish(t)

    elif mtype == 'request_map':
        if nav_node:
            nav_node.emit_last_map()

    elif mtype == 'send_goal':
        if nav_node:
            try:
                g = PoseStamped()
                g.header.stamp       = nav_node.get_clock().now().to_msg()
                g.header.frame_id    = 'map'
                g.pose.position.x    = float(msg['x'])
                g.pose.position.y    = float(msg['y'])
                g.pose.orientation.z = float(msg['z'])
                g.pose.orientation.w = float(msg['w'])
                nav_node.goal_pub.publish(g)
            except Exception as e:
                print(f"ERROR: send_goal failed: {e}")

    elif mtype == 'terminal_command':
        command = msg.get('command')
        if not command:
            return

        if terminal_process and terminal_process.poll() is None:
            try: os.killpg(os.getpgid(terminal_process.pid), signal.SIGTERM)
            except: pass

        if command == 'stop':
            bridge.send('terminal_output', {'data': "\n>>> PROCESS_TERMINATED\n"})
            return

        try:
            terminal_process = subprocess.Popen(
                f"bash -c '{command}'", shell=True,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                preexec_fn=os.setsid
            )
            def stream():
                for line in iter(terminal_process.stdout.readline, b''):
                    bridge.send('terminal_output', {'data': line.decode('utf-8', errors='replace')})
                terminal_process.stdout.close()
            threading.Thread(target=stream, daemon=True).start()
        except:
            pass


# ── ROS2 Runner ───────────────────────────────────────────────────

def start_ros2():
    global teleop_pub, nav_node
    try:
        if not rclpy.ok():
            rclpy.init()
        node       = rclpy.create_node('ros_p2p_bridge')
        teleop_pub = node.create_publisher(Twist, '/robot2/cmd_vel_cam_top', 10)
        tf_buffer  = tf2_ros.Buffer()
        tf_listener = tf2_ros.TransformListener(tf_buffer, node)

        executor = MultiThreadedExecutor(num_threads=6)  # reduced from 12 — fewer threads needed now
        nav_node = NavDataSubscriber(tf_buffer)

        nodes = [
            node,
            ImageSubscriber('web_cam_top_sub',    '/robot2/camera_top/camera_top/color/image_raw/compressed',    'camera_top',    10),
            ImageSubscriber('web_cam_bottom_sub', '/robot2/camera_bottom/camera_bottom/color/image_raw/compressed', 'camera_bottom', 10),
            BatterySubscriber(),
            nav_node,
            LogSubscriber()
        ]
        for n in nodes:
            executor.add_node(n)

        print(">>> ROS 2 Bridge ONLINE")
        executor.spin()
    except Exception as e:
        print(f"ERROR: ROS2 died: {e}", file=sys.stderr)
    finally:
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    bridge.set_on_message(on_bridge_message)
    bridge.start()
    start_ros2()