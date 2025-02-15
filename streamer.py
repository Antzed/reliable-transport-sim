# —————————————————————————————
# Part 5 
# ————————————————————————————— 

# import hashlib
# import struct
# import time
# from concurrent.futures import ThreadPoolExecutor
# from threading import Lock
# from socket import INADDR_ANY
# from lossy_socket import LossyUDP

# # Packet types
# DATA_TYPE    = 0
# ACK_TYPE     = 1
# FIN_TYPE     = 2
# FIN_ACK_TYPE = 3

# # We will store (type + seq + md5 + payload).
# #  - type: 1 byte
# #  - seq:  4 bytes (uint32)
# #  - md5:  16 bytes
# #  => 21 bytes of header
# HEADER_LEN = 1 + 4 + 16

# class Streamer:
#     def __init__(self, dst_ip, dst_port,
#                  src_ip=INADDR_ANY, src_port=0):
#         self.socket = LossyUDP()
#         self.socket.bind((src_ip, src_port))
#         self.dst_ip = dst_ip
#         self.dst_port = dst_port

#         # Sequence number management
#         self.send_seqnum      = 0   # Next sequence number to send
#         self.recv_next_seqnum = 0   # Next in-order seqnum we want to deliver at receiver

#         # For pipelining: track unacked packets by seqnum
#         # unacked[seqnum] = (packet_bytes, last_send_time)
#         self.unacked = {}

#         # Keep track of which seqnums we've received (so we can send ACK).
#         # Instead of a single last_ack_received, we do a dictionary or set.
#         # For receiving side, we only store data to deliver in order, but for
#         # sending side we track unacked in the dictionary above.
#         #
        
        
#         # Buffers & locks
#         self.recv_buffer = {}  # { seqnum -> payload }
#         self.lock        = Lock()
#         self.closed      = False

#         # Because of the 21-byte header, the largest safe payload is:
#         self.max_payload_size = 1472 - HEADER_LEN

#         # Retransmission parameters
#         self.ack_timeout = 0.25  # seconds

#         # Start up the background threads:
#         #  1) The listener (for incoming data/ACK/FIN)
#         #  2) The retransmitter (to check unacked packets)
#         self.executor = ThreadPoolExecutor(max_workers=2)
#         self.executor.submit(self.listener)
#         self.executor.submit(self.retransmitter)

#     #
#     # ─────────────────────────────────────────────────────────────
#     #   H E L P E R   F U N C T I O N S
#     # ─────────────────────────────────────────────────────────────
#     #

#     def build_packet(self, ptype: int, seqnum: int, payload: bytes) -> bytes:
#         """
#         Builds a packet of the form:
#           [type(1B)] + [seqnum(4B)] + [md5(16B)] + [payload(?B)]

#         The MD5 is computed over (type + seqnum + payload).
#         """
#         header_wo_hash = struct.pack("B", ptype) + struct.pack("I", seqnum)
#         partial_for_hash = header_wo_hash + payload

#         # Compute MD5
#         md5_val = hashlib.md5(partial_for_hash).digest()
#         # Final packet = ptype + seq + md5 + payload
#         return header_wo_hash + md5_val + payload

#     def parse_packet(self, raw_data: bytes):
#         """
#         Parses a raw packet. Returns (ptype, seqnum, payload) if the MD5 is valid,
#         or None if corrupted.

#         Packet layout:
#           [ptype(1B)] + [seqnum(4B)] + [md5(16B)] + [payload]
#         """
#         if len(raw_data) < HEADER_LEN:
#             return None  # not even enough bytes for the header
#         ptype  = raw_data[0]
#         seqnum = struct.unpack("I", raw_data[1:5])[0]
#         md5_rx = raw_data[5:21]
#         payload = raw_data[21:]

#         # Recompute MD5 to verify
#         header_wo_hash = struct.pack("B", ptype) + struct.pack("I", seqnum)
#         check_data = header_wo_hash + payload
#         md5_expected = hashlib.md5(check_data).digest()

#         if md5_rx != md5_expected:
#             # MD5 mismatch => corrupted
#             return None
#         return (ptype, seqnum, payload)

#     def send_packet(self, seqnum: int, payload: bytes):
#         """
#         Sends one packet to (self.dst_ip, self.dst_port), and stores it in self.unacked.
#         """
#         packet = self.build_packet(DATA_TYPE, seqnum, payload)
#         with self.lock:
#             self.unacked[seqnum] = (packet, time.time())  # store for possible retransmit
#         self.socket.sendto(packet, (self.dst_ip, self.dst_port))



#     def send(self, data_bytes: bytes) -> None:
#         """
#         Sends data in chunks, but does NOT block waiting for each chunk's ACK.
#         Instead, we store each packet in unacked[], and rely on the retransmitter
#         thread to handle timeouts.
#         """
#         offset = 0
#         length = len(data_bytes)

#         while offset < length:
#             chunk = data_bytes[offset : offset + self.max_payload_size]
#             offset += len(chunk)
#             # Send the chunk right away
#             seqnum = self.send_seqnum
#             self.send_seqnum += 1
#             self.send_packet(seqnum, chunk)

#         # Return immediately, letting the background retransmitter
#         # handle any needed retransmissions.

#     def recv(self) -> bytes:
#         """
#         Receives and returns the next in-sequence data chunk.
#         Blocks until that chunk is available in self.recv_buffer.
#         """
#         while True:
#             with self.lock:
#                 if self.recv_next_seqnum in self.recv_buffer:
#                     data = self.recv_buffer.pop(self.recv_next_seqnum)
#                     self.recv_next_seqnum += 1
#                     return data
#             time.sleep(0.01)  # reduce CPU churn


#     def listener(self):
#         """
#         Background thread that continuously listens for incoming packets:
#           - DATA -> add to recv_buffer if not present, send ACK
#           - ACK -> remove from unacked
#           - FIN -> reply with FIN_ACK, and record that we've gotten their FIN
#           - FIN_ACK -> used for close handshake
#         """
#         while not self.closed:
#             try:
#                 raw_data, addr = self.socket.recvfrom()
#                 if not raw_data:
#                     continue
#                 parsed = self.parse_packet(raw_data)
#                 if parsed is None:
#                     # Corrupted; discard
#                     continue

#                 ptype, seqnum, payload = parsed
#                 if ptype == DATA_TYPE:
#                     # Store in buffer (if not already stored)
#                     with self.lock:
#                         if seqnum not in self.recv_buffer:
#                             self.recv_buffer[seqnum] = payload
#                     # Send ACK
#                     ack_pkt = self.build_packet(ACK_TYPE, seqnum, b"")
#                     self.socket.sendto(ack_pkt, addr)

#                 elif ptype == ACK_TYPE:
#                     # Mark that packet as acked; remove from unacked
#                     with self.lock:
#                         if seqnum in self.unacked:
#                             del self.unacked[seqnum]

#                 elif ptype == FIN_TYPE:
#                     # They want to close. We respond with FIN_ACK
#                     finack_pkt = self.build_packet(FIN_ACK_TYPE, seqnum, b"")
#                     self.socket.sendto(finack_pkt, addr)
#                     # We can choose to keep listening until we also do our own FIN handshake
#                     # or break right away. E.g.:
#                     # break  # or continue
#                     #
#                     # A more robust logic would track "got_peer_fin = True" and only break
#                     # once we also do our own FIN handshake. But a simple approach is enough
#                     # for typical tests.

#                 elif ptype == FIN_ACK_TYPE:
#                     # Received final ack to our FIN
#                     # Possibly store that info so close() can proceed
#                     with self.lock:
#                         # We can store a special flag to say "FIN was acked"
#                         self.unacked.pop(seqnum, None)
#                         # Up to you if you want to break or keep looping
#                     # break

#             except Exception:
#                 # if we close the socket, recvfrom can throw an exception
#                 if not self.closed:
#                     pass

#     def retransmitter(self):
#         """
#         Background thread that periodically checks for unacked packets
#         whose send time is older than ack_timeout. Those are resent.
#         """
#         while not self.closed:
#             time.sleep(0.01)
#             now = time.time()
#             with self.lock:
#                 for seqnum, (packet, last_send_time) in list(self.unacked.items()):
#                     if (now - last_send_time) > self.ack_timeout:
#                         # Retransmit
#                         self.socket.sendto(packet, (self.dst_ip, self.dst_port))
#                         # Update last_send_time
#                         self.unacked[seqnum] = (packet, now)

#     def close(self) -> None:
#         """
#         A reliable close handshake:
#         1) Wait for all unacked data packets to be acked.
#         2) Send FIN.
#         3) Wait for FIN_ACK (retransmissions handled by retransmitter thread).
#         4) Allow time for any incoming FINs from the peer.
#         5) Mark closed, stop the background threads, etc.
#         """
#         # 1) Wait for unacked to be empty
#         while True:
#             with self.lock:
#                 if len(self.unacked) == 0:
#                     break
#             time.sleep(0.01)

#         # 2) Send our FIN packet
#         fin_seq = self.send_seqnum
#         self.send_seqnum += 1
#         fin_pkt = self.build_packet(FIN_TYPE, fin_seq, b"")
#         # Add it to unacked for retransmissions
#         with self.lock:
#             self.unacked[fin_seq] = (fin_pkt, time.time())
#         self.socket.sendto(fin_pkt, (self.dst_ip, self.dst_port))

#         # 3) Wait for FIN_ACK (handled by retransmitter thread)
#         while True:
#             with self.lock:
#                 if fin_seq not in self.unacked:
#                     break
#             time.sleep(0.01)

#         # 4) Give some time for the listener to process any incoming FINs
#         time.sleep(2 * self.ack_timeout)  # Wait for possible peer's FIN to arrive

#         # 5) Mark closed, stop threads
#         self.closed = True
#         self.socket.stoprecv()
#         time.sleep(0.1)
#         print("[close] Connection closed.")


# —————————————————————————————
# Extra credit
# ————————————————————————————— 


import hashlib
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from socket import INADDR_ANY
from lossy_socket import LossyUDP

# ------------------------------------------------------------------------------
# Header design (supports large seq numbers and piggybacked ACKs):
#   [ptype (1B)] + [seqnum (4B, unsigned)] + [acknum (4B, unsigned)] + [md5 (16B)] + [payload]
#
#   ptype: one of the constants below
#   seqnum: 32-bit sequence number for the *sender* of this packet
#   acknum: 32-bit piggybacked ACK for the *receiver* of this packet (or NO_ACK if none)
#   md5:    computed over (ptype + seqnum + acknum + payload)
#
#   => 1 + 4 + 4 + 16 = 25 bytes total header
# ------------------------------------------------------------------------------
HEADER_LEN = 25
NO_ACK = 0xFFFFFFFF  # sentinel meaning "no piggyback ACK"

# Packet type definitions:
DATA_NO_ACK   = 0
DATA_WITH_ACK = 1
ACK_ONLY      = 2
FIN           = 3
FIN_ACK       = 4

class Streamer:
    def __init__(self, dst_ip, dst_port, src_ip=INADDR_ANY, src_port=0):
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        # Sequence numbers
        self.send_seqnum = 0
        self.recv_next_seqnum = 0

        # Unacked packets: seq -> (packet_bytes, last_send_time)
        self.unacked = {}

        # Receive buffer: seq -> payload
        self.recv_buffer = {}

        # Nagle’s Algorithm: buffer + timestamp
        self.send_buffer = b""
        self.send_buffer_time = None

        # Delayed ACK
        self.pending_ack = NO_ACK
        self.pending_ack_time = None
        self.ack_delay = 0.05

        # Lock for shared data
        self.lock = Lock()
        self.closed = False

        # Size constraints
        self.max_payload_size = 1472 - HEADER_LEN

        # Timers for retransmission and Nagle
        self.nagle_timeout = 0.1
        self.ack_timeout = 0.25

        # Start background threads
        self.executor = ThreadPoolExecutor(max_workers=3)
        self.executor.submit(self.listener)
        self.executor.submit(self.retransmitter)
        self.executor.submit(self.sender_loop)

    # --------------------------------------------------------------------------
    # Building and parsing packets
    # --------------------------------------------------------------------------
    def build_packet(self, ptype: int, seqnum: int, payload: bytes, acknum: int = NO_ACK) -> bytes:
        """
        Build a packet:
          [ptype(1B)] + [seqnum(4B)] + [acknum(4B)] + [md5(16B)] + payload
        MD5 is computed over (ptype + seqnum + acknum + payload).
        """
        header_wo_hash = (
            struct.pack("B", ptype) +
            struct.pack("I", seqnum) +
            struct.pack("I", acknum)
        )
        partial_for_hash = header_wo_hash + payload
        md5_val = hashlib.md5(partial_for_hash).digest()
        return header_wo_hash + md5_val + payload

    def parse_packet(self, raw_data: bytes):
        if len(raw_data) < HEADER_LEN:
            return None
        ptype = raw_data[0]
        seqnum = struct.unpack("I", raw_data[1:5])[0]
        acknum = struct.unpack("I", raw_data[5:9])[0]
        md5_rx = raw_data[9:25]
        payload = raw_data[25:]

        # Recompute MD5 for verification
        header_wo_hash = raw_data[:9]  # ptype + seqnum + acknum
        check_data = header_wo_hash + payload
        md5_expected = hashlib.md5(check_data).digest()
        if md5_rx != md5_expected:
            return None
        return (ptype, seqnum, acknum, payload)

    # --------------------------------------------------------------------------
    # Sending (including Nagle and piggybacking)
    # --------------------------------------------------------------------------
    def send(self, data_bytes: bytes) -> None:
        """
        Public interface: append data to the send buffer.
        The sender_loop (via Nagle's algorithm) will flush the buffer.
        """
        with self.lock:
            if not self.send_buffer:
                self.send_buffer_time = time.time()
            self.send_buffer += data_bytes

    def send_packet(self, ptype: int, seqnum: int, payload: bytes, acknum: int = NO_ACK):
        """
        Immediately send a packet and, for types needing reliability,
        store it in unacked.
        """
        packet = self.build_packet(ptype, seqnum, payload, acknum)
        if ptype in (DATA_NO_ACK, DATA_WITH_ACK, FIN, FIN_ACK):
            with self.lock:
                self.unacked[seqnum] = (packet, time.time())
        self.socket.sendto(packet, (self.dst_ip, self.dst_port))

    def flush_send_buffer(self):
        """
        Implements Nagle’s Algorithm. Data is sent if:
          1) The buffer is at least max_payload_size,
          2) There is no unacked data (i.e. the network is idle), or
          3) The nagle_timeout has expired.
        If a delayed ACK is pending, it is piggybacked.
        """
        with self.lock:
            if not self.send_buffer:
                return

            now = time.time()
            chunk = None

            if len(self.send_buffer) >= self.max_payload_size:
                chunk = self.send_buffer[:self.max_payload_size]
                self.send_buffer = self.send_buffer[self.max_payload_size:]
            elif not self.unacked:
                chunk = self.send_buffer
                self.send_buffer = b""
            elif self.send_buffer_time and (now - self.send_buffer_time) >= self.nagle_timeout:
                chunk = self.send_buffer
                self.send_buffer = b""

            if chunk is not None:
                if self.pending_ack != NO_ACK:
                    ptype = DATA_WITH_ACK
                    piggy_ack = self.pending_ack
                    self.pending_ack = NO_ACK
                    self.pending_ack_time = None
                else:
                    ptype = DATA_NO_ACK
                    piggy_ack = NO_ACK

                seq = self.send_seqnum
                self.send_seqnum += 1
                packet = self.build_packet(ptype, seq, chunk, piggy_ack)
                self.unacked[seq] = (packet, time.time())
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                self.send_buffer_time = time.time() if self.send_buffer else None

    def sender_loop(self):
        """
        Background thread that periodically flushes the send buffer.
        """
        while not self.closed:
            self.flush_send_buffer()
            time.sleep(0.01)

    # --------------------------------------------------------------------------
    # Receiving
    # --------------------------------------------------------------------------
    def recv(self) -> bytes:
        """
        Returns the next in-order chunk from the receive buffer.
        Blocks until the expected chunk is available.
        """
        while True:
            with self.lock:
                if self.recv_next_seqnum in self.recv_buffer:
                    data = self.recv_buffer.pop(self.recv_next_seqnum)
                    self.recv_next_seqnum += 1
                    return data
            time.sleep(0.01)

    # --------------------------------------------------------------------------
    # Listener thread
    # --------------------------------------------------------------------------
    def listener(self):
        """
        Background thread that processes incoming packets:
          - DATA: store payload and schedule a delayed ACK.
          - ACK (or piggyback ACK): remove from unacked.
          - FIN: respond with FIN_ACK.
          - FIN_ACK: remove corresponding packet from unacked.
        Also, if a delayed ACK is overdue, send it as a standalone ACK.
        """
        while not self.closed:
            try:
                raw_data, addr = self.socket.recvfrom()
                if not raw_data:
                    continue

                parsed = self.parse_packet(raw_data)
                if not parsed:
                    continue

                ptype, seqnum, acknum, payload = parsed

                with self.lock:
                    # Process piggyback ACK if present
                    if acknum != NO_ACK and acknum in self.unacked:
                        del self.unacked[acknum]

                    if ptype in (DATA_NO_ACK, DATA_WITH_ACK):
                        if seqnum not in self.recv_buffer:
                            self.recv_buffer[seqnum] = payload

                        # Determine the highest contiguous sequence number received
                        needed = self.recv_next_seqnum
                        while needed in self.recv_buffer:
                            needed += 1
                        ack_val = needed - 1
                        if ack_val < 0:
                            ack_val = 0

                        self.pending_ack = ack_val
                        if self.pending_ack_time is None:
                            self.pending_ack_time = time.time()

                    elif ptype == ACK_ONLY:
                        if acknum != NO_ACK and acknum in self.unacked:
                            del self.unacked[acknum]

                    elif ptype == FIN:
                        # When a FIN is received, respond with a FIN_ACK.
                        # (Even if we're in the process of closing, respond if possible.)
                        finack_seq = self.send_seqnum
                        self.send_seqnum += 1
                        finack_pkt = self.build_packet(FIN_ACK, finack_seq, b"", NO_ACK)
                        self.unacked[finack_seq] = (finack_pkt, time.time())
                        self.socket.sendto(finack_pkt, addr)

                    elif ptype == FIN_ACK:
                        if seqnum in self.unacked:
                            del self.unacked[seqnum]

            except Exception:
                if not self.closed:
                    pass

            with self.lock:
                if (self.pending_ack != NO_ACK and 
                    self.pending_ack_time is not None and 
                    (time.time() - self.pending_ack_time) >= self.ack_delay):
                    ack_pkt = self.build_packet(ACK_ONLY, 0, b"", self.pending_ack)
                    self.socket.sendto(ack_pkt, (self.dst_ip, self.dst_port))
                    self.pending_ack = NO_ACK
                    self.pending_ack_time = None

    # --------------------------------------------------------------------------
    # Retransmission thread
    # --------------------------------------------------------------------------
    def retransmitter(self):
        """
        Background thread that checks unacked packets.
        If a packet’s last send time exceeds ack_timeout, it is retransmitted.
        """
        while not self.closed:
            time.sleep(0.01)
            now = time.time()
            with self.lock:
                for seq, (packet, last_send) in list(self.unacked.items()):
                    if (now - last_send) > self.ack_timeout:
                        self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                        self.unacked[seq] = (packet, now)

    # --------------------------------------------------------------------------
    # Close procedure with forced termination after a timeout
    # --------------------------------------------------------------------------
    def close(self) -> None:
        """
        Cleanly close the connection. This method:
          1) Waits for unacked data and the send buffer to empty (up to 5 seconds).
          2) Sends FIN and waits for FIN_ACK (up to 5 seconds).
          3) After the timeout, clears any pending FIN/FIN_ACK and forces closure.
          4) Stops background threads.
        """
        # Wait for outgoing data to be sent (up to 5 seconds)
        wait_start = time.time()
        while True:
            with self.lock:
                if not self.unacked and not self.send_buffer:
                    break
            if time.time() - wait_start > 5.0:
                break
            time.sleep(0.01)

        # Send FIN
        fin_seq = self.send_seqnum
        self.send_seqnum += 1
        fin_pkt = self.build_packet(FIN, fin_seq, b"", NO_ACK)
        with self.lock:
            self.unacked[fin_seq] = (fin_pkt, time.time())
        self.socket.sendto(fin_pkt, (self.dst_ip, self.dst_port))

        # Wait for FIN_ACK (up to 5 seconds)
        close_start = time.time()
        max_close_wait = 5.0
        while time.time() - close_start < max_close_wait:
            with self.lock:
                if fin_seq not in self.unacked:
                    break
            time.sleep(0.01)

        # Force closure by clearing any lingering unacked packets
        with self.lock:
            self.unacked.clear()

        # Mark closed so that background threads exit
        self.closed = True
        self.socket.stoprecv()
        time.sleep(0.1)
        print("[close] Connection closed.")
