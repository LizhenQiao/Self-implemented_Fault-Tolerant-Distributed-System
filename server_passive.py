"""
    Connectivity:
    1. Clients
    2. LFD
    3. Primary/Backup Servers
    4. RM

    Tips:
    1. LFD & RM : Primary/Backup same; One time connection, no need to reconnect.
    2. Clients: Primary reply; Backup do not reply.
    3. Primary/Backup Servers: Backup connect with Primary according to the membership.
"""

from socket import *
import threading
import sys
import time


# Communicate with clients
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
                print(data.decode('utf-8'))
                LOG.append(data.decode('utf-8'))
                if IS_PRIMARY[0] == 1:
                    while LOG:
                        message = LOG.pop(-1)
                        print('Client->Server: {}[Receive Data]'.format(message))
                        STATUS[0] += 1
                        print('status:{}'.format(str(STATUS[0])))
                        self.client_socket.send(message.encode('utf-8'))
                        print('Server->Client: {}[Send Data]'.format(message))
                    '''
                    if not data:
                        data = ' '
                    # Process data
                    message = data.decode('utf-8')
                    print('Client->Server: {}[Receive Data]'.format(message))
                    STATUS[0] += 1
                    print('status:{}'.format(str(STATUS[0])))
                    self.client_socket.send(message.encode('utf-8'))
                    print('Server->Client: {}[Send Data]'.format(message))
                    '''
        except KeyboardInterrupt:
            # Stop listening, close socket
            print('Server failed!')
            # Stop connection to client
            self.client_socket.close()


# Primary-Backup communication (Primary side)
class PrimaryToBackup(threading.Thread):
    def __init__(self, backup_socket, backup_address):
        threading.Thread.__init__(self)
        self.backup_socket = backup_socket
        self.backup_address = backup_address

    def run(self):
        # print('Connect with Backup (address = {})'.format(self.backup_address))
        data_connected = self.backup_socket.recv(BUFSIZ)
        self.backup_socket.send(str(STATUS[0]).encode('utf-8'))
        self.backup_socket.close()


# Primary-Backup communication (Backup side)
class BackupToPrimary(threading.Thread):
    def __init__(self, backup_address):
        threading.Thread.__init__(self)
        self.backup_address = backup_address

    def run(self):
        checkpoint_num = 0
        while True:
            if PRIMARY_INDEX[0] == INDEX[0]:
                print('I am new Primary')
                break
            try:
                primary_address = (HOST, PRIMARY_INDEX[0] + 9000)
                backup_socket = socket()
                backup_socket.bind(self.backup_address)
                backup_socket.connect(primary_address)
                # print('Connect with Primary Server!')
                data_connected = 'Backup connected'
                backup_socket.send(data_connected.encode('utf-8'))
                data = backup_socket.recv(BUFSIZ)
                checkpoint_num += 1
                STATUS[0] = int(data.decode('utf-8'))
                LOG.clear()
                print('Checkpoint num {} from Primary Server (Server{}) --> Current status: {}'
                      .format(checkpoint_num, PRIMARY_INDEX[0], STATUS[0]))
                time.sleep(CHECKPOINT_FREQ[0])
                backup_socket.close()
            except OSError:
                # print('Primary Server dead!')
                backup_socket.close()
                continue


# Get membership from RM
class ServerToRm(threading.Thread):
    def __init__(self, rm_socket):
        threading.Thread.__init__(self)
        self.rm_socket = rm_socket

    def run(self):
        # If membership changes, RM sends the new membership to servers
        print("Connect with RM...")
        while True:
            data = self.rm_socket.recv(1024)
            reply = 'Server gets the membership.'
            self.rm_socket.send(reply.encode('utf-8'))

            membership_index_list = list(data)
            if data:
                ALIVE_INDEX_LIST.clear()
                for server_index in membership_index_list:
                    ALIVE_INDEX_LIST.append(server_index)
                # Get index of Primary Server
                PRIMARY_INDEX[0] = ALIVE_INDEX_LIST[0]

                # Choose the new primary
                if ALIVE_INDEX_LIST[0] == INDEX[0]:
                    IS_PRIMARY[0] = 1
            else:
                print('Cannot receive membership from RM!')

        self.rm_socket.close()
        self.rm_socket = None


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

    LOG = []

    # membership
    ALIVE_INDEX_LIST = []

    INDEX = [0]
    INDEX[0] = int(sys.argv[1])

    # '0' : Backup; '1' : Primary
    IS_PRIMARY = [0]

    # Save the index of Primary
    PRIMARY_INDEX = [0]
    # the index of initial Primary Server
    PRIMARY_INDEX[0] = int(sys.argv[2])
    # Whether the new server is primary server
    if INDEX[0] == PRIMARY_INDEX[0]:
        IS_PRIMARY[0] = 1
        print('I am Primary Server.')
    else:
        IS_PRIMARY[0] = 0
        print('I am Backup Server.')

    # Primary-Backup checkpoint freq
    CHECKPOINT_FREQ = [0]
    CHECKPOINT_FREQ[0] = int(sys.argv[3])

    # Address of RM
    HOST = '127.0.0.1'
    RM_PORT = 6000
    RM_ADDRESS = (HOST, RM_PORT)

    # Address of Server
    PORT = 9000 + INDEX[0]
    ADDRESS = (HOST, PORT)

    # Address used to connect to Primary Server (Ask for checkpoint)
    PORT1 = 7000 + INDEX[0]
    ADDRESS1 = (HOST, PORT1)

    STATUS = [0]
    BUFSIZ = 1024

    # Create server_socket
    server_socket = socket()
    server_socket.bind(ADDRESS)
    server_socket.listen(40)

    ALIVE_INDEX_LIST.append(INDEX[0])

    print('Server{} established, waiting for messages...'.format(INDEX[0]))

    """
        Connect to RM
    """
    PORT2 = 7010 + INDEX[0]
    ADDRESS2 = (HOST, PORT2)
    tcp_rm_socket = socket()
    tcp_rm_socket.bind(ADDRESS2)
    tcp_rm_socket.connect(RM_ADDRESS)
    ServerRmThread = ServerToRm(rm_socket=tcp_rm_socket)
    ServerRmThread.start()

    while True:
        # Primary Server
        if IS_PRIMARY[0] == 1:
            try:
                while True:
                    connecting_socket, connecting_address = server_socket.accept()
                    # Create a new thread to process request
                    if connecting_address[1] == 3000 + INDEX[0]:
                        ServerLfdThread = ServerToLfd(lfd_socket=connecting_socket, lfd_address=connecting_address)
                        ServerLfdThread.start()
                    elif connecting_address[1] in [7001, 7002, 7003]:
                        PrimaryBackupThread = PrimaryToBackup(backup_socket=connecting_socket,
                                                              backup_address=connecting_address)
                        PrimaryBackupThread.start()
                    else:
                        ServerClientThread = ServerToClient(client_socket=connecting_socket, client_address=connecting_address)
                        ServerClientThread.start()
            except KeyboardInterrupt:
                # Stop listening, close socket
                print('Server failed!')
                server_socket.close()

        # Backup Server
        elif IS_PRIMARY[0] == 0:
            # Connect with Primary server
            BackupPrimaryThread = BackupToPrimary(backup_address=ADDRESS1)
            BackupPrimaryThread.start()

            try:
                while True:
                    connecting_socket, connecting_address = server_socket.accept()
                    if IS_PRIMARY[0] == 1:
                        break

                    # Create a new thread to process request
                    if connecting_address[1] == 3000 + INDEX[0]:
                        ServerLfdThread = ServerToLfd(lfd_socket=connecting_socket, lfd_address=connecting_address)
                        ServerLfdThread.start()
                    elif connecting_address[1] in [7001, 7002, 7003]:
                        break
                    else:
                        ServerClientThread = ServerToClient(client_socket=connecting_socket, client_address=connecting_address)
                        ServerClientThread.start()

            except KeyboardInterrupt:
                # Stop listening, close socket
                print('Server failed!')
                server_socket.close()