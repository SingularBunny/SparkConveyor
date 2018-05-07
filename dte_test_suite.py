from unittest import TestSuite, TextTestRunner

from class_loading_test import DynamicClassLoadingTest
from integration_test import StreamingTest
from socket_server_test import SocketServerTest


def suite():
    suite = TestSuite()
    suite.addTest(SocketServerTest('test_socket_server'))
    suite.addTest(DynamicClassLoadingTest('test_dynamic_loading'))
    suite.addTest(StreamingTest('test_streaming_mode'))
    suite.addTest(StreamingTest('test_batch_mode'))
    return suite


if __name__ == '__main__':
    runner = TextTestRunner()
    runner.run(suite())
