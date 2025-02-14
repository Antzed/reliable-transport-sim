# # do not import anything else from loss_socket besides LossyUDP
# from lossy_socket import LossyUDP
# # do not import anything else from socket except INADDR_ANY
# from socket import INADDR_ANY
# import struct
# from concurrent.futures import ThreadPoolExecutor
# from threading import Lock, Condition, Event
# import time
# import hashlib


# class Streamer:
#     # Packet type constants
#     DATA_TYPE = 0
#     ACK_TYPE = 1
#     FIN_TYPE = 2

#     def __init__(self, dst_ip, dst_port,
#                  src_ip=INADDR_ANY, src_port=0):
#         """Default values listen on all network interfaces, chooses a random source port,
#            and does not introduce any simulated packet loss."""
#         self.socket = LossyUDP()
#         self.socket.bind((src_ip, src_port))
#         self.dst_ip = dst_ip
#         self.dst_port = dst_port

#         self.sequence_num = 0  # Sender's sequence number
#         self.expected_sequence_num = 0  # Receiver's expected sequence number
#         self.buffer = {}  # Buffer for out-of-order packets (key: sequence_num, value: data)
        
#         self.buffer_lock = Lock()  # Lock for thread-safe buffer access
#         self.closed = False  # Flag to control listener thread

#         self.ack_received = Condition() # waif for acks
#         self.last_acked = -1

#         # FIN handshake state
#         self.fin_received = Event()

#         # Start background listener thread
#         self.executor = ThreadPoolExecutor(max_workers=1)
#         self.executor.submit(self.listener)

#     def listener(self):
#         """Background thread to continuously receive packets."""
#         while not self.closed:
#             try:
#                 packet, addr = self.socket.recvfrom()
#                 # changed from 5 to 21 to accommodate newly added hash
#                 if len(packet) < 21: # 1 + 4 + 16 = 21 bytes header
#                     continue
#                 ptype = packet[0]
#                 sequence_num = struct.unpack('!I', packet[1:5])[0]
#                 received_hash = packet[5:21]
#                 data = packet[21:]

#                 # Compute hash based on ptype
#                 computed_hash = hashlib.md5(data).digest() if ptype == self.DATA_TYPE else hashlib.md5(b'').digest()
                
                    
#                 if received_hash != computed_hash:
#                     print(f"Corrupted packet (seq {sequence_num}, type {ptype}) discarded")
#                     continue
                
#                 if ptype == self.DATA_TYPE:
#                     with self.buffer_lock:
#                         if sequence_num >= self.expected_sequence_num:
#                             self.buffer[sequence_num] = data
#                         #send ack
#                     ack_hash = hashlib.md5(b'').digest()
#                     ack_packet = bytes([self.ACK_TYPE]) + struct.pack('!I', sequence_num) + ack_hash
#                     self.socket.sendto(ack_packet, (self.dst_ip, self.dst_port))
#                 elif ptype == self.ACK_TYPE:
#                     with self.ack_received:
#                         if sequence_num == self.last_acked + 1:
#                             self.last_acked = sequence_num
#                             self.ack_received.notify_all()
#                 elif ptype == self.FIN_TYPE:
#                     ack_hash = hashlib.md5(b'').digest()
#                     ack_packet = bytes([self.ACK_TYPE]) + struct.pack('!I', sequence_num) + ack_hash
#                     self.socket.sendto(ack_packet, addr)
#                     self.fin_received.set()
#             except Exception as e:
#                 if not self.closed:
#                     print(f"Listener error: {e}")


#     def send(self, data_bytes: bytes) -> None:
#         """Note that data_bytes can be larger than one packet."""
#         max_chunk_size = 1472 - 21  # Reserve 5 bytes for packet type bit and the sequence number
#         for offset in range(0, len(data_bytes), max_chunk_size):
#             chunk = data_bytes[offset:offset + max_chunk_size]
#             current_sequence = self.sequence_num

#             #compute hash of data
#             chunk_hash = hashlib.md5(chunk).digest()
#             # Prepend sequence number to the chunk
#             header = bytes([self.DATA_TYPE]) + struct.pack('!I', current_sequence) + chunk_hash
#             packet = header + chunk
#             while True:
#                 self.socket.sendto(packet, (self.dst_ip, self.dst_port))
#                 with self.ack_received:
#                     if self.last_acked == current_sequence:
#                         break
#                     if not self.ack_received.wait(timeout=0.25):
#                         print(f"seq {current_sequence} timeout, retransmitting...")
#                         continue
#             self.sequence_num += 1  # Increment sequence number for next chunk

#     def recv(self) -> bytes:
#         while True:
#             with self.buffer_lock:
#                 if self.expected_sequence_num in self.buffer:
#                     data = self.buffer.pop(self.expected_sequence_num)
#                     self.expected_sequence_num += 1
#                     # Check for consecutive buffered packets
#                     while self.expected_sequence_num in self.buffer:
#                         data += self.buffer.pop(self.expected_sequence_num)
#                         self.expected_sequence_num += 1
#                     return data
#                 if self.fin_received.is_set():
#                     return b''
#             time.sleep(0.01)  # Reduce CPU usage

#     def close(self) -> None:
#         """Cleans up. It should block (wait) until the Streamer is done with all
#            the necessary ACKs and retransmissions"""
#         # Send FIN packet and wait for ACK
#         fin_seq = self.sequence_num
#         fin_hash = hashlib.md5(b'').digest()
#         fin_packet = bytes([self.FIN_TYPE]) + struct.pack('!I', fin_seq) + fin_hash

#         # retransmission until ACK received
#         while True:
#             self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
#             with self.ack_received:
#                 if self.last_acked >= fin_seq:
#                     break
#                 if not self.ack_received.wait(timeout=0.25):
#                     print("FIN time out, retransmitting")
        
#         # wait for FIND from peer then send ACK
#         if self.fin_received.wait(timeout=2):
#             print("Peer FIN not received within timeout")
        
#         time.sleep(2)

#         self.closed = True
#         self.socket.stoprecv()
#         self.executor.shutdown(wait=True)

# from lossy_socket import LossyUDP
# from socket import INADDR_ANY
# import struct
# from concurrent.futures import ThreadPoolExecutor
# from threading import Lock, Condition, Event, Timer
# import time
# import hashlib

# def current_time():
#     """Return timestamp with millisecond precision"""
#     return time.strftime("%H:%M:%S", time.localtime()) + f".{int(time.time() % 1 * 1000):03d}"

# class Streamer:
#     DATA_TYPE = 0
#     ACK_TYPE = 1
#     FIN_TYPE = 2
#     WINDOW_SIZE = 10  # Increased window size
#     RTT_ALPHA = 0.125  # Smoothing factor for RTT estimation
#     DUP_ACK_THRESHOLD = 3

#     def __init__(self, dst_ip, dst_port, src_ip=INADDR_ANY, src_port=0):
#         self.socket = LossyUDP()
#         self.socket.bind((src_ip, src_port))
#         self.dst_ip = dst_ip
#         self.dst_port = dst_port

#         # Sender state
#         self.base = 0
#         self.next_seq = 0
#         self.packet_buffer = {}  # {seq: (packet_bytes, send_time)}
#         self.timer = None
#         self.timer_lock = Lock()
#         self.estimated_rtt = 0.25  # Initial RTT estimate
#         self.tx_times = {}  # Track transmission times for RTT calculation

#         # Receiver state
#         self.recv_buffer = {}
#         self.recv_next = 0
#         self.deliver_buffer = bytearray()
#         self.duplicate_acks = 0  # Track consecutive duplicate ACKs

#         # Shared state
#         self.ack_received = Condition()
#         self.closed = False
#         self.fin_received = Event()
#         self.lock = Lock()

#         # Start background listener
#         self.executor = ThreadPoolExecutor(max_workers=1)
#         self.executor.submit(self.listener)

#     def log(self, role, message):
#         """Standardized logging with timestamp and role prefix"""
#         print(f"[{current_time()}] [{role}] {message}")

#     def listener(self):
#         while not self.closed:
#             try:
#                 packet, addr = self.socket.recvfrom()
#                 if len(packet) < 21:
#                     continue

#                 ptype = packet[0]
#                 seq_num = struct.unpack('!I', packet[1:5])[0]
#                 received_hash = packet[5:21]
#                 data = packet[21:]

#                 if ptype == self.DATA_TYPE:
#                     computed_hash = hashlib.md5(data).digest()
#                     if received_hash != computed_hash:
#                         self.log("RECV", f"Corrupted packet seq={seq_num} (exp_hash={computed_hash.hex()[:6]} vs rcvd_hash={received_hash.hex()[:6]})")
#                         # Send duplicate ACK
#                         with self.lock:
#                             ack_num = self.recv_next - 1
#                             self.log("RECV", f"Sending DUP-ACK {ack_num} (last_good={self.recv_next})")
#                             ack_packet = bytes([self.ACK_TYPE]) + struct.pack('!I', ack_num) + hashlib.md5(b'').digest()
#                             self.socket.sendto(ack_packet, addr)
#                         continue

#                     with self.lock:
#                         if seq_num >= self.recv_next:
#                             self.recv_buffer[seq_num] = data
#                             self.log("RECV", f"Buffered seq={seq_num} (window={self.recv_next}-{self.recv_next + self.WINDOW_SIZE})")
#                         else:
#                             self.log("RECV", f"Ignored old/duplicate seq={seq_num} (expecting {self.recv_next})")

#                         # Deliver in-order packets
#                         delivered = []
#                         while self.recv_next in self.recv_buffer:
#                             delivered.append(self.recv_next)
#                             self.deliver_buffer += self.recv_buffer.pop(self.recv_next)
#                             self.recv_next += 1
                        
#                         if delivered:
#                             self.log("RECV", f"Delivered sequences {delivered[0]}-{delivered[-1]} (next_expect={self.recv_next})")

#                         ack_num = self.recv_next - 1
#                         self.log("RECV", f"Sending cumulative ACK {ack_num} (buffer_size={len(self.recv_buffer)})")
#                         ack_packet = bytes([self.ACK_TYPE]) + struct.pack('!I', ack_num) + hashlib.md5(b'').digest()
#                         self.socket.sendto(ack_packet, addr)

#                 elif ptype == self.ACK_TYPE:
#                     with self.ack_received:
#                         if seq_num >= self.base:
#                             # Calculate RTT for this acknowledgment
#                             if seq_num in self.tx_times:
#                                 rtt = time.time() - self.tx_times[seq_num]
#                                 self.estimated_rtt = (1 - self.RTT_ALPHA) * self.estimated_rtt + self.RTT_ALPHA * rtt
#                                 del self.tx_times[seq_num]
#                                 self.log("SEND", f"RTT updated to {self.estimated_rtt:.3f}s for seq={seq_num}")

#                             new_base = seq_num + 1
#                             for s in range(self.base, new_base):
#                                 if s in self.packet_buffer:
#                                     del self.packet_buffer[s]
#                             self.log("SEND", f"ACK {seq_num} advanced window {self.base}->{new_base} (next_seq={self.next_seq})")
#                             self.base = new_base
                            
#                             if self.base < self.next_seq:
#                                 self.start_timer()
#                             else:
#                                 self.stop_timer()
#                             self.ack_received.notify_all()

#                 elif ptype == self.FIN_TYPE:
#                     ack_packet = bytes([self.ACK_TYPE]) + struct.pack('!I', seq_num) + hashlib.md5(b'').digest()
#                     self.socket.sendto(ack_packet, addr)
#                     self.fin_received.set()

#             except Exception as e:
#                 if not self.closed:
#                     self.log("ERROR", f"Listener error: {e}")

#     def send(self, data_bytes: bytes) -> None:
#         max_chunk_size = 1472 - 21
#         chunks = [data_bytes[i:i+max_chunk_size] for i in range(0, len(data_bytes), max_chunk_size)]
        
#         with self.lock:
#             for chunk in chunks:
#                 while self.next_seq >= self.base + self.WINDOW_SIZE:
#                     with self.ack_received:
#                         self.log("SEND", f"Window full (base={self.base}, next_seq={self.next_seq}), waiting...")
#                         self.ack_received.wait()
                
#                 seq = self.next_seq
#                 chunk_hash = hashlib.md5(chunk).digest()
#                 packet = bytes([self.DATA_TYPE]) + struct.pack('!I', seq) + chunk_hash + chunk
#                 self.packet_buffer[seq] = (packet, time.time(), 0)
#                 self.tx_times[seq] = time.time()
#                 self.socket.sendto(packet, (self.dst_ip, self.dst_port))
#                 self.log("SEND", f"TX seq={seq} (base={self.base}, window_used={self.next_seq - self.base}/{self.WINDOW_SIZE})")
#                 self.next_seq += 1
#             self.start_timer()

#     def start_timer(self):
#         with self.timer_lock:
#             if not self.timer or not self.timer.is_alive():
#                 timeout = min(2 * self.estimated_rtt, 1.0)  # Dynamic timeout based on RTT
#                 self.timer = Timer(timeout, self.retransmit)
#                 self.timer.start()
#                 self.log("SEND", f"Timer started with timeout {timeout:.3f}s")

#     def stop_timer(self):
#         with self.timer_lock:
#             if self.timer:
#                 self.timer.cancel()
#                 self.log("SEND", "Timer stopped")

#     def retransmit(self):
#         with self.lock:
#             unacked = [s for s in range(self.base, self.next_seq) if s in self.packet_buffer]
#             if unacked:
#                 self.log("SEND", f"Retransmitting {len(unacked)} packets: {unacked[0]}-{unacked[-1]}")
#                 for seq in unacked:
#                     packet, _ = self.packet_buffer[seq]
#                     self.socket.sendto(packet, (self.dst_ip, self.dst_port))
#                     self.log("SEND", f"RETX seq={seq} (attempt_count={self.packet_buffer[seq][2]})")
#                     # Update transmission time and count
#                     self.packet_buffer[seq] = (packet, time.time(), self.packet_buffer[seq][2]+1)
#             self.start_timer()

#     def recv(self) -> bytes:
#         while True:
#             with self.lock:
#                 if len(self.deliver_buffer) > 0:
#                     data = bytes(self.deliver_buffer)
#                     self.deliver_buffer = bytearray()
#                     self.log("RECV", f"Delivering {len(data)} bytes to application")
#                     return data
#                 if self.fin_received.is_set():
#                     return b''
#             time.sleep(0.01)

#     def close(self) -> None:
#         with self.ack_received:
#             while self.base < self.next_seq:
#                 self.log("SEND", f"Waiting for ACKs (unacked={self.next_seq - self.base})")
#                 self.ack_received.wait()
        
#         fin_seq = self.next_seq
#         fin_packet = bytes([self.FIN_TYPE]) + struct.pack('!I', fin_seq) + hashlib.md5(b'').digest()
        
#         attempts = 0
#         while True:
#             self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
#             self.log("SEND", f"Sent FIN seq={fin_seq} (attempt={attempts})")
#             with self.ack_received:
#                 if self.base > fin_seq:
#                     break
#                 if not self.ack_received.wait(timeout=0.25):
#                     attempts += 1
#         self.fin_received.wait(timeout=2)
#         time.sleep(2)
#         self.closed = True
#         self.socket.stoprecv()
#         self.executor.shutdown(wait=True)
#         self.log("SEND", "Connection closed gracefully")

import hashlib
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from socket import INADDR_ANY
from lossy_socket import LossyUDP

# Packet types
DATA_TYPE    = 0
ACK_TYPE     = 1
FIN_TYPE     = 2
FIN_ACK_TYPE = 3

# We will store (type + seq + md5 + payload).
#  - type: 1 byte
#  - seq:  4 bytes (uint32)
#  - md5:  16 bytes
#  => 21 bytes of header
HEADER_LEN = 1 + 4 + 16

class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        # Sequence number management
        self.send_seqnum      = 0   # Next sequence number to send
        self.recv_next_seqnum = 0   # Next in-order seqnum we want to deliver at receiver

        # For pipelining: track unacked packets by seqnum
        # unacked[seqnum] = (packet_bytes, last_send_time)
        self.unacked = {}

        # Keep track of which seqnums we've received (so we can send ACK).
        # Instead of a single last_ack_received, we do a dictionary or set.
        # For receiving side, we only store data to deliver in order, but for
        # sending side we track unacked in the dictionary above.
        #
        
        
        # Buffers & locks
        self.recv_buffer = {}  # { seqnum -> payload }
        self.lock        = Lock()
        self.closed      = False

        # Because of the 21-byte header, the largest safe payload is:
        self.max_payload_size = 1472 - HEADER_LEN

        # Retransmission parameters
        self.ack_timeout = 0.25  # seconds

        # Start up the background threads:
        #  1) The listener (for incoming data/ACK/FIN)
        #  2) The retransmitter (to check unacked packets)
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.executor.submit(self.listener)
        self.executor.submit(self.retransmitter)

    #
    # ─────────────────────────────────────────────────────────────
    #   H E L P E R   F U N C T I O N S
    # ─────────────────────────────────────────────────────────────
    #

    def build_packet(self, ptype: int, seqnum: int, payload: bytes) -> bytes:
        """
        Builds a packet of the form:
          [type(1B)] + [seqnum(4B)] + [md5(16B)] + [payload(?B)]

        The MD5 is computed over (type + seqnum + payload).
        """
        header_wo_hash = struct.pack("B", ptype) + struct.pack("I", seqnum)
        partial_for_hash = header_wo_hash + payload

        # Compute MD5
        md5_val = hashlib.md5(partial_for_hash).digest()
        # Final packet = ptype + seq + md5 + payload
        return header_wo_hash + md5_val + payload

    def parse_packet(self, raw_data: bytes):
        """
        Parses a raw packet. Returns (ptype, seqnum, payload) if the MD5 is valid,
        or None if corrupted.

        Packet layout:
          [ptype(1B)] + [seqnum(4B)] + [md5(16B)] + [payload]
        """
        if len(raw_data) < HEADER_LEN:
            return None  # not even enough bytes for the header
        ptype  = raw_data[0]
        seqnum = struct.unpack("I", raw_data[1:5])[0]
        md5_rx = raw_data[5:21]
        payload = raw_data[21:]

        # Recompute MD5 to verify
        header_wo_hash = struct.pack("B", ptype) + struct.pack("I", seqnum)
        check_data = header_wo_hash + payload
        md5_expected = hashlib.md5(check_data).digest()

        if md5_rx != md5_expected:
            # MD5 mismatch => corrupted
            return None
        return (ptype, seqnum, payload)

    def send_packet(self, seqnum: int, payload: bytes):
        """
        Sends one packet to (self.dst_ip, self.dst_port), and stores it in self.unacked.
        """
        packet = self.build_packet(DATA_TYPE, seqnum, payload)
        with self.lock:
            self.unacked[seqnum] = (packet, time.time())  # store for possible retransmit
        self.socket.sendto(packet, (self.dst_ip, self.dst_port))



    def send(self, data_bytes: bytes) -> None:
        """
        Sends data in chunks, but does NOT block waiting for each chunk's ACK.
        Instead, we store each packet in unacked[], and rely on the retransmitter
        thread to handle timeouts.
        """
        offset = 0
        length = len(data_bytes)

        while offset < length:
            chunk = data_bytes[offset : offset + self.max_payload_size]
            offset += len(chunk)
            # Send the chunk right away
            seqnum = self.send_seqnum
            self.send_seqnum += 1
            self.send_packet(seqnum, chunk)

        # Return immediately, letting the background retransmitter
        # handle any needed retransmissions.

    def recv(self) -> bytes:
        """
        Receives and returns the next in-sequence data chunk.
        Blocks until that chunk is available in self.recv_buffer.
        """
        while True:
            with self.lock:
                if self.recv_next_seqnum in self.recv_buffer:
                    data = self.recv_buffer.pop(self.recv_next_seqnum)
                    self.recv_next_seqnum += 1
                    return data
            time.sleep(0.01)  # reduce CPU churn


    def listener(self):
        """
        Background thread that continuously listens for incoming packets:
          - DATA -> add to recv_buffer if not present, send ACK
          - ACK -> remove from unacked
          - FIN -> reply with FIN_ACK, and record that we've gotten their FIN
          - FIN_ACK -> used for close handshake
        """
        while not self.closed:
            try:
                raw_data, addr = self.socket.recvfrom()
                if not raw_data:
                    continue
                parsed = self.parse_packet(raw_data)
                if parsed is None:
                    # Corrupted; discard
                    continue

                ptype, seqnum, payload = parsed
                if ptype == DATA_TYPE:
                    # Store in buffer (if not already stored)
                    with self.lock:
                        if seqnum not in self.recv_buffer:
                            self.recv_buffer[seqnum] = payload
                    # Send ACK
                    ack_pkt = self.build_packet(ACK_TYPE, seqnum, b"")
                    self.socket.sendto(ack_pkt, addr)

                elif ptype == ACK_TYPE:
                    # Mark that packet as acked; remove from unacked
                    with self.lock:
                        if seqnum in self.unacked:
                            del self.unacked[seqnum]

                elif ptype == FIN_TYPE:
                    # They want to close. We respond with FIN_ACK
                    finack_pkt = self.build_packet(FIN_ACK_TYPE, seqnum, b"")
                    self.socket.sendto(finack_pkt, addr)
                    # We can choose to keep listening until we also do our own FIN handshake
                    # or break right away. E.g.:
                    # break  # or continue
                    #
                    # A more robust logic would track "got_peer_fin = True" and only break
                    # once we also do our own FIN handshake. But a simple approach is enough
                    # for typical tests.

                elif ptype == FIN_ACK_TYPE:
                    # Received final ack to our FIN
                    # Possibly store that info so close() can proceed
                    with self.lock:
                        # We can store a special flag to say "FIN was acked"
                        self.unacked.pop(seqnum, None)
                        # Up to you if you want to break or keep looping
                    # break

            except Exception:
                # if we close the socket, recvfrom can throw an exception
                if not self.closed:
                    pass

    def retransmitter(self):
        """
        Background thread that periodically checks for unacked packets
        whose send time is older than ack_timeout. Those are resent.
        """
        while not self.closed:
            time.sleep(0.01)
            now = time.time()
            with self.lock:
                for seqnum, (packet, last_send_time) in list(self.unacked.items()):
                    if (now - last_send_time) > self.ack_timeout:
                        # Retransmit
                        self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                        # Update last_send_time
                        self.unacked[seqnum] = (packet, now)

    def close(self) -> None:
        """
        A reliable close handshake:
        1) Wait for all unacked data packets to be acked.
        2) Send FIN.
        3) Wait for FIN_ACK (retransmissions handled by retransmitter thread).
        4) Allow time for any incoming FINs from the peer.
        5) Mark closed, stop the background threads, etc.
        """
        # 1) Wait for unacked to be empty
        while True:
            with self.lock:
                if len(self.unacked) == 0:
                    break
            time.sleep(0.01)

        # 2) Send our FIN packet
        fin_seq = self.send_seqnum
        self.send_seqnum += 1
        fin_pkt = self.build_packet(FIN_TYPE, fin_seq, b"")
        # Add it to unacked for retransmissions
        with self.lock:
            self.unacked[fin_seq] = (fin_pkt, time.time())
        self.socket.sendto(fin_pkt, (self.dst_ip, self.dst_port))

        # 3) Wait for FIN_ACK (handled by retransmitter thread)
        while True:
            with self.lock:
                if fin_seq not in self.unacked:
                    break
            time.sleep(0.01)

        # 4) Give some time for the listener to process any incoming FINs
        time.sleep(2 * self.ack_timeout)  # Wait for possible peer's FIN to arrive

        # 5) Mark closed, stop threads
        self.closed = True
        self.socket.stoprecv()
        time.sleep(0.1)
        print("[close] Connection closed.")
