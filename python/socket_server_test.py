import socket
import threading
import unittest
from unittest import TestCase
from unittest.mock import Mock

from python.socket_server import SocketServer, RequestHandler

SOCKET_HOST = 'localhost'
SOCKET_PORT = 9998


class SocketServerTest(TestCase):

    def setUp(self):
        super().setUp()

        self.stop_event = Mock()
        self.restart_event = Mock()

        self.server = SocketServer((SOCKET_HOST, SOCKET_PORT),
                                   RequestHandler,
                                   self.stop_event,
                                   self.restart_event)

        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.start()

    def tearDown(self):
        if not self.server.is_shut_down():
            self.server.shutdown()
        self.server.server_close()
        super().tearDown()

    def test_socket_server(self):
        with socket.create_connection((SOCKET_HOST, SOCKET_PORT)) as sock:
            sock.sendall(bytes('some', 'utf-8'))
            received = str(sock.recv(1024), 'utf-8')
            self.assertTrue(received == SocketServer.Messages.UNKNOWN_MESSAGE_RESPONSE)

        with socket.create_connection((SOCKET_HOST, SOCKET_PORT)) as sock:
            sock.sendall(bytes(SocketServer.Messages.RESTART_STREAMING_SIGNAL, 'utf-8'))
            received = str(sock.recv(1024), 'utf-8')
            self.assertTrue(received == SocketServer.Messages.RESTART_STREAMING_RESPONSE)
            self.restart_event.set.assert_called_once_with()

        with socket.create_connection((SOCKET_HOST, SOCKET_PORT)) as sock:
            sock.sendall(bytes(SocketServer.Messages.STOP_SIGNAL, 'utf-8'))
            self.server_thread.join()
            received = str(sock.recv(1024), 'utf-8')
            self.stop_event.set.assert_called_once_with()
            self.assertTrue(received == SocketServer.Messages.STOP_RESPONSE)
            self.assertTrue(self.server.is_shut_down())


if __name__ == '__main__':
    unittest.main()
