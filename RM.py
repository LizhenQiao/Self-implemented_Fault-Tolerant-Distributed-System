"""
    Replication Manager
    Responsibility:
    1. Keep track of all current and past membership views
    2. Create factories to create replicas (Unavailable now)

    More to accomplish:
    1. Send message to server factories to create replicas
    2. Loop and detect membership change to create backup replicas
    3. Record views of membership

    Connectivity:
    1. GFD
    2. Clients
    3. Servers
"""

from socket import *
import threading

# TODO: should derive from ClientThread


class GFDThread(threading.Thread):
    def __init__(self, GFD_socket, GFD_address, GFD_name):
        threading.Thread.__init__(self)
        self.socket = GFD_socket
        self.address = GFD_address
        self.name = GFD_name

    def run(self):
        # Receive update of membership from GFD
        print('[RM] {}(address = {}) connects!'.format(self.name, self.address))

        try:
            while True:
                # Receive data from GFD and block until data arrives
                data = self.socket.recv(BUF_SIZE)
                membership_index_list = list(data)
                if data:
                    # Process data: update membership
                    ALIVE_LIST.clear()
                    for index in membership_index_list:
                        ALIVE_LIST.append('Server{}'.format(index))
                    print('[RM] Receive message from {}, update membership! '
                          'Current membership: {}'.format(self.name, ALIVE_LIST))

                else:
                    print('{}(address = {}) disconnects!'.format(self.name, self.address))
                    break
        finally:
            # Stop connection to client
            self.socket.close()
            self.socket = None


class ClientThread(threading.Thread):
    def __init__(self, client_socket, client_address, client_name):
        threading.Thread.__init__(self)
        self.socket = client_socket
        self.address = client_address
        self.name = client_name

    def run(self):
        # Receive membership request from client
        print('[RM] {}(address = {}) connects!'.format(self.name, self.address))

        try:
            while True:
                # Receive data from client and block until data arrives
                data = self.socket.recv(BUF_SIZE)
                if data:
                    # Process data: send back membership
                    # Convert Alive_list to list of integers (index of members)
                    membership_index_list = [int(server_name[-1]) for server_name in ALIVE_LIST]

                    # Send membership list to client
                    client_socket.send(bytes(membership_index_list))
                    print('[RM] Receive message from {}, send membership! '
                          'Current membership: {}'.format(self.name, ALIVE_LIST))
                else:
                    print('{}(address = {}) disconnects!'.format(self.name, self.address))
                    break
        finally:
            # Stop connection to client
            self.socket.close()
            self.socket = None


if __name__ == '__main__':
    # Configuration of RM socket
    HOST = '127.0.0.1'
    RM_PORT = 6000
    BUF_SIZE = 1024
    RM_ADDRESS = (HOST, RM_PORT)

    ALIVE_LIST = []

    print('RM established...')
    print('RM working...')
    print('WARNING: because we run the application in one computer, '
          'we use timestamp to distinguish different entities, '
          'so START GFD FIRST and START CLIENTS AFTER.\n')

    print('[RM] Current membership -> Server Count:{}, Server: {}'.format(len(ALIVE_LIST), ALIVE_LIST))
    tcp_rm_socket = socket()
    tcp_rm_socket.bind(RM_ADDRESS)
    tcp_rm_socket.listen(5)

    """
        First connect to GFD and remember the port.
    """
    tcp_gfd_socket, gfd_address = tcp_rm_socket.accept()
    gfd_thread = GFDThread(GFD_socket=tcp_gfd_socket, GFD_address=gfd_address,
                           GFD_name='GFD')
    gfd_thread.start()

    """
        Connect to client
    """
    client_index = 1
    while True:
        client_socket, client_address = tcp_rm_socket.accept()

        client_thread = ClientThread(client_socket=client_socket, client_address=client_address,
                                     client_name='Client{}'.format(client_index))
        client_index += 1
        client_thread.start()
