"""
    Connectivity:
    1. Clients
    2. LFD
"""

from socket import *
import threading
import sys


class ServerToClient(threading.Thread):
    def __init__(self, client_socket, client_address):
        threading.Thread.__init__(self)
        self.client_socket = client_socket
        self.client_address = client_address

    def run(self):

        print('Connecting with Client (address = {})!'.format(self.client_address))

        try:
            while True:
                data = self.client_socket.recv(1024)
                if not data:
                    data = ' '
                # Process data
                message = data.decode('utf-8')
                print('Client->Server: {}[Receive Data]'.format(message))
                STATUS[0] += 1
                print('status:{}'.format(STATUS[0]))
                self.client_socket.send(message.encode('utf-8'))
                print('Server->Client: {}[Send Data]'.format(message))
        except KeyboardInterrupt:
            # Stop listening, close socket
            print('Server failed!')

            # Stop connection to client
            self.client_socket.close()


class ServerToLfd(threading.Thread):
    def __init__(self, lfd_socket, lfd_address):
        threading.Thread.__init__(self)
        self.lfd_socket = lfd_socket
        self.lfd_address = lfd_address

    def run(self):
        data = self.lfd_socket.recv(1024)
        reply = '[LFD] Server is alive.'
        self.lfd_socket.send(reply.encode('utf-8'))
        self.lfd_socket.close()


if __name__ == '__main__':

    index = [0]
    index[0] = int(sys.argv[1])

    HOST = '127.0.0.1'
    PORT = 9000 + index[0]
    BUFSIZ = 1024
    ADDRESS = (HOST, PORT)
    STATUS = [0]

    server_socket = socket()
    server_socket.bind(ADDRESS)
    server_socket.listen(20)

    print('Server{} established, waiting for messages...'.format(index[0]))
    try:
        while True:
            connecting_socket, connecting_address = server_socket.accept()

            # Create a new thread to process request
            if connecting_address[1] == 3000 + index[0]:
                ServerLfdThread = ServerToLfd(lfd_socket=connecting_socket, lfd_address=connecting_address)
                ServerLfdThread.start()
            else:
                ServerClientThread = ServerToClient(client_socket=connecting_socket, client_address=connecting_address)
                ServerClientThread.start()

    except KeyboardInterrupt:
        # Stop listening, close socket
        print('Server failed!')
        server_socket.close()
