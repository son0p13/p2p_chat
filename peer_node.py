import socket
import threading
import json
import time
import sys
import tkinter as tk
from tkinter import scrolledtext, messagebox
import queue

BOOTSTRAP_IP = '127.0.0.1'
BOOTSTRAP_PORT = 5000

class Peer:
    def __init__(self, port, ui_queue):
        # Fix lỗi 5: tự động detect IP thực thay vì hardcode 127.0.0.1
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            self.ip = s.getsockname()[0]
            s.close()
        except Exception:
            self.ip = '127.0.0.1'

        self.port = port
        self.known_peers = []
        self.available_groups = {}  # group_name -> (leader_ip, leader_port)
        self.my_groups = {}         # group_name -> {'members': set of (ip, port), 'is_leader': bool}
        self.lock = threading.Lock()
        self.ui_queue = ui_queue

    def recv_full(self, conn):
        """Fix lỗi 3: Đọc toàn bộ dữ liệu cho đến khi parse JSON thành công"""
        chunks = []
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
            try:
                json.loads(b''.join(chunks).decode('utf-8'))
                break
            except json.JSONDecodeError:
                continue
        return b''.join(chunks).decode('utf-8')

    def start_server(self):
        """Vai trò Server: Lắng nghe tin nhắn đến đồng thời với việc gửi đi"""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', self.port))
        server.listen(5)
        while True:
            conn, addr = server.accept()
            threading.Thread(target=self.handle_incoming, args=(conn, addr)).start()

    def handle_incoming(self, conn, addr):
        try:
            data = self.recv_full(conn)  # Fix lỗi 3: thay recv(1024)
            if data:
                msg = json.loads(data)
                if msg['type'] == 'CHAT':
                    self.ui_queue.put(('chat', f"[Tin nhắn 1-1 từ {msg['sender']}]: {msg['content']}"))
                elif msg['type'] == 'GROUP_CHAT':
                    self.ui_queue.put(('chat', f"[Nhóm '{msg['group']}' - từ {msg['sender']}]: {msg['content']}"))
                elif msg['type'] == 'JOIN_REQUEST':
                    sender_ip, sender_port = msg['sender'].split(':')
                    self.ui_queue.put(('join_request', {
                        'sender_ip': sender_ip,
                        'sender_port': int(sender_port),
                        'group': msg['group']}))
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
        except json.JSONDecodeError:
            print(f"[!] handle_incoming: JSON không hợp lệ từ {addr}")
        except KeyError as e:
            print(f"[!] handle_incoming: thiếu trường {e} từ {addr}")
        except Exception as e:
            print(f"[!] handle_incoming: lỗi từ {addr}: {e}")
        finally:
            conn.close()

    def heartbeat_loop(self):
        """Yêu cầu 3.4 & 3.5: Cơ chế Peer Discovery và Cập nhật trạng thái liên tục"""
        while True:
            s = None
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
            except Exception:
                pass
            finally:
                if s:
                    s.close()
            time.sleep(5)

    def _send_payload(self, target_ip, target_port, payload):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3)
            s.connect((target_ip, target_port))
            s.send(json.dumps(payload).encode('utf-8'))
            s.close()
            return True
        except socket.error:
            if payload.get('type') in ('CHAT', 'GROUP_CHAT'):
                self.ui_queue.put(('chat', f"[!] Giao tiếp thất bại với {target_ip}:{target_port}."))
            return False

    def send_message(self, target_ip, target_port, content, msg_type='CHAT', group_name=None):
        payload = {
            'type': msg_type,
            'sender': f"{self.ip}:{self.port}",
            'content': content
        }
        if group_name:
            payload['group'] = group_name
        return self._send_payload(target_ip, target_port, payload)

    def create_group(self, group_name):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((BOOTSTRAP_IP, BOOTSTRAP_PORT))
            s.send(json.dumps({'type': 'CREATE_GROUP', 'port': self.port, 'group': group_name}).encode('utf-8'))
            response = json.loads(s.recv(4096).decode('utf-8'))
            s.close()
            if response.get('type') == 'CREATE_GROUP_ERROR':
                self.ui_queue.put(('chat', f"[!] {response['reason']}"))
                return False
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
        self.send_message(leader_ip, leader_port, content="", msg_type='JOIN_REQUEST', group_name=group_name)
        self.ui_queue.put(('chat', f"[*] Đã gửi yêu cầu tham gia nhóm '{group_name}' tới trưởng nhóm."))

    def accept_join_request(self, sender_ip, sender_port, group_name):
        # Fix lỗi 1: tách I/O ra ngoài lock, tránh deadlock
        members_to_notify = []
        new_member = (sender_ip, sender_port)
        with self.lock:
            if group_name not in self.my_groups or not self.my_groups[group_name]['is_leader']:
                return
            members_to_notify = list(self.my_groups[group_name]['members'])
            self.my_groups[group_name]['members'].add(new_member)

        for member_ip, member_port in members_to_notify:
            if (member_ip, member_port) != (self.ip, self.port):
                self._send_payload(member_ip, member_port, {
                    'type': 'NEW_MEMBER',
                    'group': group_name,
                    'new_member': list(new_member)
                })

        self._send_payload(sender_ip, sender_port, {
            'type': 'JOIN_ACCEPT',
            'group': group_name,
            'members': [list(m) for m in members_to_notify + [new_member]]
        })
        self.ui_queue.put(('chat', f"[*] Đã chấp nhận {sender_ip}:{sender_port} vào nhóm '{group_name}'."))

    def reject_join_request(self, sender_ip, sender_port, group_name):
        self._send_payload(sender_ip, sender_port, {
            'type': 'JOIN_REJECT',
            'group': group_name
        })

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
        right_frame = tk.Frame(self.root)
        right_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=(0, 10), pady=10)

        left_frame = tk.Frame(self.root)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(10, 0), pady=10)

        self.chat_display = scrolledtext.ScrolledText(left_frame, state='disabled', height=15)
        self.chat_display.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        msg_frame = tk.Frame(left_frame)
        msg_frame.pack(fill=tk.X, pady=2)
        tk.Label(msg_frame, text="Tin nhắn:").pack(side=tk.LEFT)
        self.msg_entry = tk.Entry(msg_frame)
        self.msg_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

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

        group_frame = tk.Frame(left_frame)
        group_frame.pack(fill=tk.X, pady=2)
        tk.Label(group_frame, text="Tên nhóm:").pack(side=tk.LEFT)
        self.group_entry = tk.Entry(group_frame, width=12)
        self.group_entry.pack(side=tk.LEFT, padx=5)
        tk.Button(group_frame, text="Tạo nhóm", command=self.create_group).pack(side=tk.LEFT, padx=2)
        tk.Button(group_frame, text="Xin vào", command=self.join_group).pack(side=tk.LEFT, padx=2)
        tk.Button(group_frame, text="Gửi Nhóm", command=self.send_group).pack(side=tk.LEFT, padx=2)

        tk.Label(right_frame, text="Peers Online").pack()
        self.peer_list = tk.Listbox(right_frame, width=20, height=10)
        self.peer_list.pack(fill=tk.Y, expand=True, pady=(0, 10))
        self.peer_list.bind('<<ListboxSelect>>', self.on_peer_select)

        tk.Label(right_frame, text="Nhóm hiện có").pack()
        self.group_list = tk.Listbox(right_frame, width=20, height=10)
        self.group_list.pack(fill=tk.Y, expand=True)

    def process_queue(self):
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
        self.root.after(100, self.process_queue)

    def on_peer_select(self, event):
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
            messagebox.showerror("Lỗi", "Không thể tạo nhóm (đã tồn tại hoặc lỗi kết nối)!")

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

        # Fix lỗi 4: chạy vòng lặp gửi trong thread riêng, không block GUI
        def _do_send(peer_list, grp, msg):
            for peer_ip, peer_port in peer_list:
                if (peer_ip, peer_port) != (self.peer.ip, self.peer.port):
                    self.peer.send_message(peer_ip, peer_port, msg, msg_type='GROUP_CHAT', group_name=grp)

        threading.Thread(target=_do_send, args=(peers, grp, msg), daemon=True).start()
        self.msg_entry.delete(0, tk.END)


if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 6001
    root = tk.Tk()
    app = ChatGUI(root, port)
    root.mainloop()