"""
    Global Fault Detector
    Responsibility:
    1. Heartbeat LFDs
    2. Keep record of membership based on liveliness of LFDs
    3. Report to RM about current membership

    Connectivity:
    1. RM
    2. LFDs
"""

from socket import *
import threading
import sys
import time


def send_membership_to_rm(rm_socket):
    # Convert Alive_list to list of integers (index of members)
    membership_index_list = [int(server_name[-1]) for server_name in ALIVE_LIST]

    # Send membership list to RM
    rm_socket.send(bytes(membership_index_list))


class LFDThread(threading.Thread):
    def __init__(self, _socket, _address, rm_socket):
        threading.Thread.__init__(self)
        self.socket = _socket
        self.address = _address
        self.rm_socket = rm_socket

    def run(self):
        print('Connecting with lfd {} (address = {})!'.format((self.address[1] - 2000), self.address))

        # Communicate with lfds
        while True:
            message = '(from gfd to lfd) Are you alive?'
            self.socket.send(message.encode('utf-8'))
            reply = self.socket.recv(1024)

            if reply:
                # update Alive_list
                if reply.decode('utf-8')[8:13] == 'alive':
                    if reply.decode('utf-8')[:7] not in ALIVE_LIST:
                        ALIVE_LIST.append(reply.decode('utf-8')[:7])
                        print('Membership changed.')
                        print('[GFD] Current membership -> Server Count:{}, Server: {}'
                              .format(len(ALIVE_LIST), ALIVE_LIST))
                        # Send membership info to RM
                        try:
                            if self.rm_socket:
                                send_membership_to_rm(self.rm_socket)
                            else:
                                print("Connection to RM has already been disabled!")
                        except ConnectionRefusedError:
                            # Stop connection to client
                            self.rm_socket.close()
                            self.rm_socket = None
                            print("RM is dead!")

                elif reply.decode('utf-8')[8:12] == 'dead':
                    if reply.decode('utf-8')[:7] in ALIVE_LIST:
                        ALIVE_LIST.remove(reply.decode('utf-8')[:7])
                        print('Membership changed.')
                        print('[GFD] Current membership -> Server Count:{}, Server: {}'
                              .format(len(ALIVE_LIST), ALIVE_LIST))
                        # Send membership info to RM
                        try:
                            if self.rm_socket:
                                send_membership_to_rm(self.rm_socket)
                            else:
                                print("Connection to RM has already been disabled!")
                        except ConnectionRefusedError:
                            # Stop connection to client
                            self.rm_socket.close()
                            self.rm_socket = None
                            print("RM is dead!")

                time.sleep(gfd_freq/1000)
            else:
                # Stop connection to client
                self.socket.close()


if __name__ == "__main__":
    # Address of gfd
    HOST = '127.0.0.1'
    GFD_PORT = 8000
    RM_PORT = 6000
    BUFSIZ = 1024
    GFD_ADDRESS = (HOST, GFD_PORT)
    RM_ADDRESS = (HOST, RM_PORT)

    try:
        gfd_freq = int(sys.argv[1])
    except IndexError:
        print('Invalid check frequency of Global Fault Detector.')
        print('Please restart.')
    ALIVE_LIST = []

    gfd_socket = socket()
    gfd_socket.bind(GFD_ADDRESS)
    gfd_socket.listen(20)

    tcp_rm_socket = socket()

    print('GFD established...\n')
    print('GFD working...\n')

    try:
        # Connect to RM to construct the fault-tolerance system
        while True:
            try:
                # TODO: currently not designate a specific port --> client will also need to connect
                # because no other device other than GFD connects to RM
                tcp_rm_socket.connect(RM_ADDRESS)
                print('Connecting with RM (address = {})!'.format(RM_ADDRESS))

                # Send initial state to RM
                try:
                    send_membership_to_rm(tcp_rm_socket)
                except ConnectionRefusedError:
                    # Stop connection to client
                    tcp_rm_socket.close()
                    print("RM is dead!")
                break
            except ConnectionRefusedError:
                print("[GFD] No RM is running now!")
                time.sleep(2)

        while True:
            connecting_socket, connecting_address = gfd_socket.accept()

            # Create a new thread to process request
            socket_thread = LFDThread(_socket=connecting_socket,
                                      _address=connecting_address,
                                      rm_socket=tcp_rm_socket)
            socket_thread.start()

    except KeyboardInterrupt:
        # Stop listening, close socket
        print('GFD stops!')
        gfd_socket.close()
        gfd_socket = None
