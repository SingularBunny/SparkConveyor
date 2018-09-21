from socketserver import BaseRequestHandler, TCPServer
from threading import Thread


class RequestHandler(BaseRequestHandler):
    """
    The RequestHandler class for job stopping.
    """

    def handle(self):
        self.data = self.request.recv(1024).strip()
        # just send back the same data, but upper-cased
        if bytes(SocketServer.Messages.RESTART_STREAMING_SIGNAL, 'utf-8') == self.data:
            self.request.sendall(bytes(SocketServer.Messages.RESTART_STREAMING_RESPONSE, 'utf-8'))
            self.server.restart_event.set()
        elif bytes(SocketServer.Messages.STOP_SIGNAL, 'utf-8') == self.data:
            self.request.sendall(bytes(SocketServer.Messages.STOP_RESPONSE, 'utf-8'))
            self.server.stop_event.set()
            Thread(target=self.server.shutdown).start()
        else:
            self.request.sendall(bytes(SocketServer.Messages.UNKNOWN_MESSAGE_RESPONSE, 'utf-8'))


class SocketServer(TCPServer):
    class Messages:
        """
        Socket Server service messages.
        """
        STOP_SIGNAL = 'stop_pe'
        STOP_RESPONSE = 'job_stopping'
        RESTART_STREAMING_SIGNAL = 'restart_streaming'
        RESTART_STREAMING_RESPONSE = 'restart'
        UNKNOWN_MESSAGE_RESPONSE = 'unknown_command'

    def __init__(self, server_address,
                 request_handler_class,
                 stop_event,
                 restart_event,
                 bind_and_activate=True):
        super().__init__(server_address, request_handler_class, bind_and_activate)
        self.stop_event = stop_event
        self.restart_event = restart_event

    def is_shut_down(self):
        return self._BaseServer__is_shut_down.is_set()
