# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Condition, Event
import time


class Streamer:
    # Packet type constants
    DATA_TYPE = 0
    ACK_TYPE = 1
    FIN_TYPE = 2

    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.sequence_num = 0  # Sender's sequence number
        self.expected_sequence_num = 0  # Receiver's expected sequence number
        self.buffer = {}  # Buffer for out-of-order packets (key: sequence_num, value: data)
        
        self.buffer_lock = Lock()  # Lock for thread-safe buffer access
        self.expected_seq_lock = Lock()
        self.closed = False  # Flag to control listener thread

        self.ack_received = Condition() # waif for acks
        self.last_acked = -1

        # FIN handshake state
        self.fin_received = Event()

        # Thread-safe Structure
        self.buffer_lock = Lock()

        # Start background listener thread
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)

    def listener(self):
        """Background thread to continuously receive packets."""
        while not self.closed:
            try:
                packet, addr = self.socket.recvfrom()
                # changed from 4 to 5 to accommodate the package type: is it data or ack
                if len(packet) < 5:
                    continue
                ptype = packet[0]
                sequence_num = struct.unpack('!I', packet[1:5])[0]

                if ptype == self.DATA_TYPE:
                    data = packet[5:]
                    with self.buffer_lock:
                        if sequence_num >= self.expected_sequence_num:
                            self.buffer[sequence_num] = data
                        #send ack
                    ack_packet = bytes([self.ACK_TYPE]) + struct.pack('!I', sequence_num)
                    self.socket.sendto(ack_packet, (self.dst_ip, self.dst_port))
                elif ptype == self.ACK_TYPE:
                    with self.ack_received:
                        if sequence_num > self.last_acked:
                            self.last_acked = sequence_num
                            self.ack_received.notify_all()
                elif ptype == self.FIN_TYPE:
                    ack_packet = bytes([self.ACK_TYPE]) + struct.pack('!I', sequence_num)
                    self.socket.sendto(ack_packet, addr)
                    self.fin_received.set()
            except Exception as e:
                if not self.closed:
                    print(f"Listener error: {e}")
                break


    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        max_chunk_size = 1472 - 5  # Reserve 5 bytes for packet type bit and the sequence number
        for offset in range(0, len(data_bytes), max_chunk_size):
            chunk = data_bytes[offset:offset + max_chunk_size]
            current_sequence = self.sequence_num
            # Prepend sequence number to the chunk
            header = bytes([self.DATA_TYPE]) + struct.pack('!I', current_sequence)
            packet = header + chunk
            while True:
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                with self.ack_received:
                    if self.last_acked >= current_sequence:
                        break
                    if not self.ack_received.wait(timeout=0.25):
                        print(f"seq {current_sequence} timeout, retransmitting...")
                        continue
            self.sequence_num += 1  # Increment sequence number for next chunk

    def recv(self) -> bytes:
        while True:
            with self.expected_seq_lock, self.buffer_lock:
                if self.expected_sequence_num in self.buffer:
                    data = self.buffer.pop(self.expected_sequence_num)
                    self.expected_sequence_num += 1
                    # Check for consecutive buffered packets
                    while self.expected_sequence_num in self.buffer:
                        data += self.buffer.pop(self.expected_sequence_num)
                        self.expected_sequence_num += 1
                    return data
                if self.fin_received.is_set():
                    return b''
            time.sleep(0.01)  # Reduce CPU usage

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # Send FIN packet and wait for ACK
        fin_seq = self.sequence_num
        fin_packet = bytes([self.FIN_TYPE]) + struct.pack('!I', fin_seq)

        # retransmission until ACK received
        while True:
            self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
            with self.ack_received:
                if self.last_acked >= fin_seq:
                    break
                if not self.ack_received.wait(timeout=0.25):
                    print("FIN time out, retransmitting")
        
        # wait for FIND from peer then send ACK
        if self.fin_received.wait(timeout=2):
            print("Peer FIN not received within timeout")
        
        time.sleep(2)

        self.closed = True
        self.socket.stoprecv()
        self.executor.shutdown(wait=True)