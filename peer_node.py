import socket
import threading
import json
import time
import sys
import uuid
import tkinter as tk
from tkinter import scrolledtext, messagebox
import queue

BOOTSTRAP_IP = '127.0.0.1'
BOOTSTRAP_PORT = 5000
SEND_RETRIES = 3
RETRYABLE_MESSAGE_TYPES = ('CHAT', 'GROUP_CHAT', 'JOIN_REQUEST', 'JOIN_ACCEPT', 'JOIN_REJECT', 'NEW_MEMBER')

class Peer:
    def __init__(self, port, ui_queue):
        self.ip = '127.0.0.1'
        self.port = port
        self.known_peers = []
        self.available_groups = {} # group_name -> (leader_ip, leader_port)
        self.my_groups = {} # group_name -> {'members': set of (ip, port), 'is_leader': bool}
        self.lock = threading.Lock()
        self.ui_queue = ui_queue

    def _make_message(self, msg_type, content="", group_name=None, **extra_fields):
        payload = {
            'type': msg_type,
            'sender': f"{self.ip}:{self.port}",
            'message_id': str(uuid.uuid4()),
            'timestamp': time.time(),
            'content': content
        }
        if group_name:
            payload['group'] = group_name
        payload.update(extra_fields)
        return payload

    def _send_response(self, conn, msg_type, message_id=None, error=None):
        response = {
            'type': msg_type,
            'message_id': message_id,
            'timestamp': time.time()
        }
        if error:
            response['error'] = error
        conn.send(json.dumps(response).encode('utf-8'))

    def _validate_message(self, msg):
        if not isinstance(msg, dict):
            return False, "Message must be a JSON object"
        if not msg.get('type'):
            return False, "Missing message type"
        tracked_types = ('CHAT', 'GROUP_CHAT', 'JOIN_REQUEST', 'JOIN_ACCEPT', 'JOIN_REJECT', 'NEW_MEMBER')
        if msg['type'] in tracked_types:
            for field in ('sender', 'message_id', 'timestamp'):
                if field not in msg:
                    return False, f"Missing field: {field}"
        if msg['type'] in ('CHAT', 'GROUP_CHAT') and 'content' not in msg:
            return False, "Missing field: content"
        if msg['type'] in ('GROUP_CHAT', 'JOIN_REQUEST', 'JOIN_ACCEPT', 'JOIN_REJECT', 'NEW_MEMBER') and 'group' not in msg:
            return False, "Missing field: group"
        return True, None

    def start_server(self):
        """Vai trò Server: Lắng nghe tin nhắn đến đồng thời với việc gửi đi"""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('0.0.0.0', self.port))
        server.listen(5)
        while True:
            conn, addr = server.accept()
            threading.Thread(target=self.handle_incoming, args=(conn, addr)).start()

    def handle_incoming(self, conn, addr):
        try:
            data = conn.recv(4096).decode('utf-8')
            if data:
                msg = json.loads(data)
                is_valid, error = self._validate_message(msg)
                if not is_valid:
                    self._send_response(conn, 'ERROR', msg.get('message_id'), error)
                    return
                if msg['type'] == 'CHAT':
                    self.ui_queue.put(('chat', f"[Tin nhắn 1-1 từ {msg['sender']}]: {msg['content']}"))
                elif msg['type'] == 'GROUP_CHAT':
                    self.ui_queue.put(('chat', f"[Nhóm '{msg['group']}' - từ {msg['sender']}]: {msg['content']}"))
                elif msg['type'] == 'JOIN_REQUEST':
                    sender_ip, sender_port = msg['sender'].split(':')
                    self.ui_queue.put(('join_request', {'sender_ip': sender_ip, 'sender_port': int(sender_port), 'group': msg['group']}))
                elif msg['type'] == 'JOIN_ACCEPT':
                    group = msg['group']
                    members = set([tuple(m) for m in msg['members']])
                    with self.lock:
                        self.my_groups[group] = {'members': members, 'is_leader': False}
                    self.ui_queue.put(('chat', f"[*] Đã tham gia nhóm '{group}' thành công."))
                elif msg['type'] == 'JOIN_REJECT':
                    self.ui_queue.put(('chat', f"[!] Yêu cầu tham gia nhóm '{msg['group']}' bị từ chối."))
                elif msg['type'] == 'NEW_MEMBER':
                    group = msg['group']
                    new_member = tuple(msg['new_member'])
                    with self.lock:
                        if group in self.my_groups:
                            self.my_groups[group]['members'].add(new_member)
                    self.ui_queue.put(('chat', f"[*] {new_member[0]}:{new_member[1]} đã tham gia nhóm '{group}'."))
                else:
                    self._send_response(conn, 'ERROR', msg.get('message_id'), f"Unsupported message type: {msg['type']}")
                    return

                self._send_response(conn, 'ACK', msg.get('message_id'))
        except Exception as e:
            try:
                self._send_response(conn, 'ERROR', None, str(e))
            except Exception:
                pass
        finally:
            conn.close()

    def heartbeat_loop(self):
        """Yêu cầu 3.4 & 3.5: Cơ chế Peer Discovery và Cập nhật trạng thái liên tục"""
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                s.connect((BOOTSTRAP_IP, BOOTSTRAP_PORT))
                s.send(json.dumps({'type': 'HEARTBEAT', 'port': self.port}).encode('utf-8'))
                
                response = s.recv(4096).decode('utf-8')
                if response:
                    msg = json.loads(response)
                    if msg.get('type') == 'PEER_LIST':
                        with self.lock:
                            self.known_peers = [(p['ip'], p['port']) for p in msg['peers'] if p['port'] != self.port]
                            if 'groups' in msg:
                                self.available_groups = {g: tuple(addr) for g, addr in msg['groups'].items()}
                        self.ui_queue.put(('peers', self.known_peers))
                        self.ui_queue.put(('groups', self.available_groups))
            except Exception as e:
                pass
            finally:
                s.close()
            time.sleep(5)  # Gửi heartbeat (nhịp tim) mỗi 5 giây

    def _send_payload_once(self, target_ip, target_port, payload):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.settimeout(3)
            s.connect((target_ip, target_port))
            s.send(json.dumps(payload).encode('utf-8'))
            response_data = s.recv(4096).decode('utf-8')
        finally:
            s.close()

        if not response_data:
            return False, "Khong nhan duoc phan hoi"

        response = json.loads(response_data)
        if response.get('type') == 'ACK' and response.get('message_id') == payload.get('message_id'):
            return True, None
        if response.get('type') == 'ERROR':
            return False, response.get('error', 'Khong ro loi')
        return False, "Phan hoi khong hop le"

    def _send_payload(self, target_ip, target_port, payload):
        max_attempts = SEND_RETRIES if payload.get('type') in RETRYABLE_MESSAGE_TYPES else 1
        last_error = "Khong ro loi"

        for attempt in range(1, max_attempts + 1):
            should_retry = attempt < max_attempts
            try:
                success, error = self._send_payload_once(target_ip, target_port, payload)
                if success:
                    return True
                last_error = error or last_error
                if error and error not in ("Khong nhan duoc phan hoi", "Phan hoi khong hop le"):
                    self.ui_queue.put(('chat', f"[!] Peer {target_ip}:{target_port} bao loi: {last_error}"))
                    return False
            except (socket.error, socket.timeout) as e:
                last_error = str(e) or "Loi ket noi"
            except (json.JSONDecodeError, UnicodeDecodeError):
                last_error = "Phan hoi khong hop le"

            if should_retry:
                time.sleep(0.2)

        if payload.get('type') in RETRYABLE_MESSAGE_TYPES:
            self.ui_queue.put(('chat', f"[!] Gui that bai sau {max_attempts} lan toi {target_ip}:{target_port}: {last_error}"))
        return False

    def send_message(self, target_ip, target_port, content, msg_type='CHAT', group_name=None):
        payload = self._make_message(msg_type, content=content, group_name=group_name)
        return self._send_payload(target_ip, target_port, payload)
        
    def create_group(self, group_name):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((BOOTSTRAP_IP, BOOTSTRAP_PORT))
            s.send(json.dumps({'type': 'CREATE_GROUP', 'port': self.port, 'group': group_name}).encode('utf-8'))
            s.recv(4096)
            s.close()
            with self.lock:
                self.my_groups[group_name] = {'members': {(self.ip, self.port)}, 'is_leader': True}
            return True
        except Exception:
            return False

    def request_join_group(self, group_name):
        with self.lock:
            if group_name in self.my_groups:
                self.ui_queue.put(('chat', f"[*] Bạn đã ở trong nhóm '{group_name}' rồi."))
                return
            if group_name not in self.available_groups:
                self.ui_queue.put(('chat', f"[!] Nhóm '{group_name}' không tồn tại."))
                return
            leader_ip, leader_port = self.available_groups[group_name]
            
        join_request_sent = self.send_message(leader_ip, leader_port, content="", msg_type='JOIN_REQUEST', group_name=group_name)
        if not join_request_sent:
            self.ui_queue.put(('chat', f"[!] Khong gui duoc yeu cau tham gia nhom '{group_name}' toi truong nhom."))
            return
        self.ui_queue.put(('chat', f"[*] Đã gửi yêu cầu tham gia nhóm '{group_name}' tới trưởng nhóm."))

    def accept_join_request(self, sender_ip, sender_port, group_name):
        with self.lock:
            if group_name in self.my_groups and self.my_groups[group_name]['is_leader']:
                new_member = (sender_ip, sender_port)
                for member_ip, member_port in self.my_groups[group_name]['members']:
                    if (member_ip, member_port) != (self.ip, self.port):
                        self._send_payload(
                            member_ip,
                            member_port,
                            self._make_message('NEW_MEMBER', group_name=group_name, new_member=new_member)
                        )
                
                self.my_groups[group_name]['members'].add(new_member)
                
                self._send_payload(
                    sender_ip,
                    sender_port,
                    self._make_message('JOIN_ACCEPT', group_name=group_name, members=list(self.my_groups[group_name]['members']))
                )
                self.ui_queue.put(('chat', f"[*] Đã chấp nhận {sender_ip}:{sender_port} vào nhóm '{group_name}'."))

    def reject_join_request(self, sender_ip, sender_port, group_name):
        self._send_payload(
            sender_ip,
            sender_port,
            self._make_message('JOIN_REJECT', group_name=group_name)
        )

    def start_threads(self):
        threading.Thread(target=self.start_server, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        self.ui_queue.put(('chat', f"[*] Peer khởi chạy thành công tại {self.ip}:{self.port}"))

class ChatGUI:
    def __init__(self, root, port):
        self.root = root
        self.root.title(f"P2P Chat Node - Port {port}")
        self.root.geometry("600x450")
        
        self.ui_queue = queue.Queue()
        self.peer = Peer(port, self.ui_queue)
        
        self.setup_ui()
        self.peer.start_threads()
        self.process_queue()

    def setup_ui(self):
        # --- Bố cục chính ---
        # Khung danh sách Peer bên phải (được pack trước để cố định chiều rộng)
        right_frame = tk.Frame(self.root)
        right_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=(0, 10), pady=10)

        # Khung chat bên trái
        left_frame = tk.Frame(self.root)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(10, 0), pady=10)
        
        # --- Các thành phần trong khung bên trái (left_frame) ---
        self.chat_display = scrolledtext.ScrolledText(left_frame, state='disabled', height=15)
        self.chat_display.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # Khung nhập tin nhắn
        msg_frame = tk.Frame(left_frame)
        msg_frame.pack(fill=tk.X, pady=2)
        tk.Label(msg_frame, text="Tin nhắn:").pack(side=tk.LEFT)
        self.msg_entry = tk.Entry(msg_frame)
        self.msg_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        # Khung gửi 1-1
        direct_frame = tk.Frame(left_frame)
        direct_frame.pack(fill=tk.X, pady=2)
        tk.Label(direct_frame, text="IP:").pack(side=tk.LEFT)
        self.ip_entry = tk.Entry(direct_frame, width=12)
        self.ip_entry.insert(0, "127.0.0.1")
        self.ip_entry.pack(side=tk.LEFT, padx=5)
        tk.Label(direct_frame, text="Port:").pack(side=tk.LEFT)
        self.port_entry = tk.Entry(direct_frame, width=6)
        self.port_entry.pack(side=tk.LEFT, padx=5)
        tk.Button(direct_frame, text="Gửi 1-1", command=self.send_direct).pack(side=tk.LEFT, padx=5)
        
        # Khung gửi Nhóm
        group_frame = tk.Frame(left_frame)
        group_frame.pack(fill=tk.X, pady=2)
        tk.Label(group_frame, text="Tên nhóm:").pack(side=tk.LEFT)
        self.group_entry = tk.Entry(group_frame, width=12)
        self.group_entry.pack(side=tk.LEFT, padx=5)
        tk.Button(group_frame, text="Tạo nhóm", command=self.create_group).pack(side=tk.LEFT, padx=2)
        tk.Button(group_frame, text="Xin vào", command=self.join_group).pack(side=tk.LEFT, padx=2)
        tk.Button(group_frame, text="Gửi Nhóm", command=self.send_group).pack(side=tk.LEFT, padx=2)

        # --- Các thành phần trong khung bên phải (right_frame) ---
        tk.Label(right_frame, text="Peers Online").pack()
        self.peer_list = tk.Listbox(right_frame, width=20, height=10)
        self.peer_list.pack(fill=tk.Y, expand=True, pady=(0, 10))
        self.peer_list.bind('<<ListboxSelect>>', self.on_peer_select)
        
        tk.Label(right_frame, text="Nhóm hiện có").pack()
        self.group_list = tk.Listbox(right_frame, width=20, height=10)
        self.group_list.pack(fill=tk.Y, expand=True)

    def process_queue(self):
        """Hàm cập nhật giao diện định kỳ mà không block luồng xử lý chính"""
        try:
            while True:
                msg_type, data = self.ui_queue.get_nowait()
                if msg_type == 'chat':
                    self.chat_display.config(state='normal')
                    self.chat_display.insert(tk.END, data + "\n")
                    self.chat_display.config(state='disabled')
                    self.chat_display.yview(tk.END)
                elif msg_type == 'peers':
                    self.peer_list.delete(0, tk.END)
                    for p in data:
                        self.peer_list.insert(tk.END, f"{p[0]}:{p[1]}")
                elif msg_type == 'groups':
                    self.group_list.delete(0, tk.END)
                    for g in data:
                        self.group_list.insert(tk.END, g)
                elif msg_type == 'join_request':
                    sender_ip = data['sender_ip']
                    sender_port = data['sender_port']
                    group = data['group']
                    if messagebox.askyesno("Yêu cầu tham gia", f"Peer {sender_ip}:{sender_port} muốn tham gia nhóm '{group}'. Đồng ý?"):
                        self.peer.accept_join_request(sender_ip, sender_port, group)
                    else:
                        self.peer.reject_join_request(sender_ip, sender_port, group)
        except queue.Empty:
            pass
        # Cứ 100ms kiểm tra hàng đợi một lần
        self.root.after(100, self.process_queue)

    def on_peer_select(self, event):
        """Tự động điền IP và Port khi click vào một peer trong danh sách"""
        selection = self.peer_list.curselection()
        if selection:
            peer_info = self.peer_list.get(selection[0])
            ip, port = peer_info.split(':')
            self.ip_entry.delete(0, tk.END)
            self.ip_entry.insert(0, ip)
            self.port_entry.delete(0, tk.END)
            self.port_entry.insert(0, port)

    def send_direct(self):
        ip = self.ip_entry.get()
        port_str = self.port_entry.get()
        msg = self.msg_entry.get()
        if not ip or not port_str or not msg:
            messagebox.showwarning("Lỗi", "Vui lòng nhập IP, Port và Tin nhắn!")
            return
        port = int(port_str)
        self.chat_display.config(state='normal')
        self.chat_display.insert(tk.END, f"[Bạn -> {ip}:{port}]: {msg}\n")
        self.chat_display.config(state='disabled')
        self.chat_display.yview(tk.END)
        self.peer.send_message(ip, port, msg)
        self.msg_entry.delete(0, tk.END)

    def create_group(self):
        grp = self.group_entry.get()
        if not grp:
            messagebox.showwarning("Lỗi", "Vui lòng nhập Tên nhóm!")
            return
        if self.peer.create_group(grp):
            self.chat_display.config(state='normal')
            self.chat_display.insert(tk.END, f"[*] Đã tạo nhóm '{grp}' thành công.\n")
            self.chat_display.config(state='disabled')
            self.chat_display.yview(tk.END)
        else:
            messagebox.showerror("Lỗi", "Không thể kết nối đến Bootstrap Server!")

    def join_group(self):
        grp = self.group_entry.get()
        if not grp:
            messagebox.showwarning("Lỗi", "Vui lòng nhập Tên nhóm!")
            return
        self.peer.request_join_group(grp)

    def send_group(self):
        grp = self.group_entry.get()
        msg = self.msg_entry.get()
        if not grp or not msg:
            messagebox.showwarning("Lỗi", "Vui lòng nhập Tên nhóm và Tin nhắn!")
            return
        
        with self.peer.lock:
            if grp not in self.peer.my_groups:
                messagebox.showwarning("Lỗi", f"Bạn chưa tham gia nhóm '{grp}'!")
                return
            peers = list(self.peer.my_groups[grp]['members'])
            
        self.chat_display.config(state='normal')
        self.chat_display.insert(tk.END, f"[Bạn -> Nhóm '{grp}']: {msg}\n")
        self.chat_display.config(state='disabled')
        self.chat_display.yview(tk.END)
        
        for peer_ip, peer_port in peers:
            if (peer_ip, peer_port) != (self.peer.ip, self.peer.port):
                self.peer.send_message(peer_ip, peer_port, msg, msg_type='GROUP_CHAT', group_name=grp)
        self.msg_entry.delete(0, tk.END)

if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 6001
    root = tk.Tk()
    app = ChatGUI(root, port)
    root.mainloop()
