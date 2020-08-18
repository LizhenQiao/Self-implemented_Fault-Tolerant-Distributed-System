"""
    Client
    Responsibility:
    1. Send messages to server

    Connectivity:
    1. RM
    2. Servers (replication)
"""

from socket import *
import json
import sys
import threading
import time


class ServerThread(threading.Thread):
    def __init__(self, server_socket, server_message, server_address, server_name):
        threading.Thread.__init__(self)
        self.socket = server_socket
        self.message = server_message
        self.address = server_address
        self.name = server_name

    def run(self):
        with CV:
            try:
                while True:
                    # Wait until main thread notifies and less than three messages are in the list
                    # and have not sent before
                    while len(RECEIVED_MESSAGE_LIST) >= len(CONNECTED_SERVER_PORT_DICT) \
                            or CONNECTED_SERVER_PORT_DICT[self.address[1]]:
                        print('Thread for {}, current state:\n'
                              'RECEIVED_MESSAGE_LIST:{}\n'
                              'CONNECTED_SERVER_PORT_DICT:{}\n'.format(self.name, RECEIVED_MESSAGE_LIST,
                                                                       CONNECTED_SERVER_PORT_DICT))
                        CV.wait()

                    # Communicate with the server
                    print('Process message from {}'.format(self.name))
                    self.message = MESSAGE_TO_SERVERS
                    self.socket.send(self.message.encode('utf-8'))
                    reply = self.socket.recv(1024)

                    # Return message to main thread and wait
                    RECEIVED_MESSAGE_LIST.append(reply.decode('utf-8'))
                    CONNECTED_SERVER_PORT_DICT[self.address[1]] = True  # Send message in this turn
                    CV.notify()

            # If server dies, delete the port in CONNECTED_SERVER_PORT_LIST
            # and terminate the thread
            except OSError:
                print('OSError')
                CONNECTED_SERVER_PORT_DICT.pop(self.address[1])
                self.socket = None


if __name__ == '__main__':
    # Configuration
    # index of client 
    index = int(sys.argv[1])

    # Ports of servers which are connected with client
    # {server_port: have sent message in this turn}
    CONNECTED_SERVER_PORT_DICT = {}

    host = '127.0.0.1'

    # Address of RM
    RM_PORT = 6000
    RM_ADDRESS = (host, RM_PORT)

    BUFSIZ = 1024

    tcp_rm_socket = socket()

    """
        Try connect to RM
    """
    while True:
        try:
            print('[Client] Connecting with RM (address = {})!'.format(RM_ADDRESS))
            tcp_rm_socket.connect(RM_ADDRESS)
            break
        except ConnectionRefusedError:
            print('[Client] RM is dead!')
            time.sleep(2)

    """
        Send message to servers
    """
    CV = threading.Condition()
    RECEIVED_MESSAGE_LIST = []
    with CV:
        while True:
            # Counter of client's messages
            filename = 'D:/Github/Project_18749/client{}_counter.json'.format(index)
            with open(filename) as f:
                counter = json.load(f)

            # Get message
            data = input('>')
            if not data:
                data = ' '
            MESSAGE_TO_SERVERS = 'Client {} - Request {} - {}'.format(index, '{0:03d}'.format(counter), data)
            counter += 1

            with open(filename, 'w') as f:
                json.dump(counter, f)

            # Ask RM for membership
            message_to_rm = '[Client] Please give me the membership.'
            tcp_rm_socket.send(message_to_rm.encode('utf-8'))  # TODO: dead RM?
            data = tcp_rm_socket.recv(BUFSIZ)
            membership_index_list = list(data)
            current_server_port_list = [server_index + 9000 for server_index in membership_index_list]

            # If membership changes, start/ close a new thread.
            # Closed threads will be closed automatically, no extra action needed
            for server_port in current_server_port_list:
                if server_port not in CONNECTED_SERVER_PORT_DICT:
                    CONNECTED_SERVER_PORT_DICT[server_port] = False
                    server_address = (host, server_port)
                    server_index = server_port - 9000
                    server_name = 'Server{}'.format(server_index)

                    # Establish new socket with the specific server
                    tcp_server_socket = socket()
                    while True:
                        try:
                            print('[Client] Connecting with server (address = {})!'.format(server_address))
                            tcp_server_socket.connect(server_address)
                            break
                        except ConnectionRefusedError:
                            # print('[Client] Server is dead!')
                            time.sleep(2)

                    server_thread = ServerThread(server_socket=tcp_server_socket, server_address=server_address,
                                                 server_message=MESSAGE_TO_SERVERS, server_name=server_name)
                    server_thread.start()

            # Notify all alive threads to send messages
            CV.notify_all()

            # Wait until server thread notifies and all servers receive messages
            while len(RECEIVED_MESSAGE_LIST) < len(CONNECTED_SERVER_PORT_DICT):
                CV.wait()

            # Receive all messages from alive threads
            # Duplicate detection
            previous_message = None
            for message in RECEIVED_MESSAGE_LIST:
                if not previous_message:
                    print('[Client] Receive message: {}'.format(message))
                    previous_message = message
                else:
                    if previous_message == message:
                        print('[Client] Duplicated message detected: {}'.format(message))

            RECEIVED_MESSAGE_LIST = []
            for server_port in CONNECTED_SERVER_PORT_DICT:
                CONNECTED_SERVER_PORT_DICT[server_port] = False  # Clean state
