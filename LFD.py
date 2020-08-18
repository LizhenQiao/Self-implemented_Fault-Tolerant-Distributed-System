"""
    Local Fault Detector
    Responsibility:
    1. Heartbeat server of the same index to check its liveliness
    2. Be monitored by GFD about the liveliness itself

    Connectivity:
    1. GFD
    2. Server
"""

from socket import *
import sys
import time
import threading


class ServerThread(threading.Thread):
    def __init__(self, lfd_address, server_address):
        threading.Thread.__init__(self)
        self.lfd_address = lfd_address
        self.server_address = server_address

    def run(self):
        while True:
            try:
                # Bind to LFD (server -> heartbeat, GFD -> membership)
                tcp_server_socket = socket(AF_INET, SOCK_STREAM)
                tcp_server_socket.bind(self.lfd_address)

                tcp_server_socket.connect(self.server_address)

                # Send data
                tcp_server_socket.send(message.encode('utf-8'))

                # Receive data
                reply = tcp_server_socket.recv(BUFSIZ)

                # Confirm server is alive
                print(reply.decode('utf-8'))
                ALIVE[2] = ALIVE[1]
                ALIVE[1] = ALIVE[0]
                ALIVE[0] = 1

            except:
                print("[LFD] Server is dead!")
                ALIVE[2] = ALIVE[1]
                ALIVE[1] = ALIVE[0]
                ALIVE[0] = 0

            finally:
                heartbeat_counter[0] += 1
                print('[LFD] heartbeat freq:1 heartbeat/{} seconds; heartbeat count:{}\n'
                      .format(int(check_rate / 1000), heartbeat_counter[0]))
                time.sleep(check_rate / 1000)
                tcp_server_socket.close()


class GFDThread(threading.Thread):
    def __init__(self, lfd_address, gfd_address):
        threading.Thread.__init__(self)
        self.lfd_address = lfd_address
        self.gfd_address = gfd_address

    def run(self):
        # connect with GFD
        message_alive = 'Server{} alive.'.format(index[0])
        message_dead = 'Server{} dead.'.format(index[0])

        tcp_gfd_socket = socket(AF_INET, SOCK_STREAM)
        tcp_gfd_socket.bind(self.lfd_address)

        try:
            tcp_gfd_socket.connect(self.gfd_address)
        except ConnectionRefusedError:
            print('[LFD] Cannot connect with GFD!')
            tcp_gfd_socket.close()

        # Send Server's status to GFD
        try:
            while True:
                data = tcp_gfd_socket.recv(1024)
                if ALIVE[0] == ALIVE[1] == 1:
                    tcp_gfd_socket.send(message_alive.encode('utf-8'))
                elif ALIVE[0] == ALIVE[1] == 0:
                    tcp_gfd_socket.send(message_dead.encode('utf-8'))
                else:
                    if ALIVE[2] == 1:
                        tcp_gfd_socket.send(message_alive.encode('utf-8'))
                    else:
                        tcp_gfd_socket.send(message_dead.encode('utf-8'))
        except OSError:
            pass
        tcp_gfd_socket.close()


if __name__ == '__main__':
    index = [0]
    index[0] = int(sys.argv[2])

    '''
        Configuration
    '''
    HOST = '127.0.0.1'
    BUFSIZ = 1024

    SERVER_PORT = 9000 + index[0]
    SERVER_ADDRESS = (HOST, SERVER_PORT)

    GFD_PORT = 8000
    GFD_ADDRESS = (HOST, GFD_PORT)

    LFD_PORT_GFD = 2000 + index[0]  # TODO: managed by replica manager/ GFD
    LFD_ADDRESS_GFD = (HOST, LFD_PORT_GFD)
    LFD_PORT_S = 3000 + index[0]  # TODO: managed by replica manager/ GFD
    LFD_ADDRESS_S = (HOST, LFD_PORT_S)

    lfd_name = "LFD{}".format(index[0])  # TODO
    server_name = "Server{}".format(index[0])
    message = "Are you alive?"

    heartbeat_counter = [0]
    # A parameter which claims server alive or not. {'1':'alive'; '0':'dead'}
    ALIVE = [0, 0, 0]

    # Check frequency
    try:
        check_rate = int(sys.argv[1])
        if check_rate == 0:
            check_rate = 500
    except IndexError:
        print('No frequency input! Use the default value 500!')
        check_rate = 500
    except ValueError:
        print('Frequency value should be an integer! Use the default value 500!')
        check_rate = 500

    '''
        Start running
    '''
    print('LFD{} runs!'.format(index[0]))

    try:
        # establish two threads
        server_thread = ServerThread(lfd_address=LFD_ADDRESS_S, server_address=SERVER_ADDRESS)
        gfd_thread = GFDThread(lfd_address=LFD_ADDRESS_GFD, gfd_address=GFD_ADDRESS)

        server_thread.start()
        gfd_thread.start()

    except KeyboardInterrupt:
        print('Local fault detector stops!')
