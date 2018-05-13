import socket
import threading
import unittest
import time

import happybase
import os

from hdfs3 import HDFileSystem
from pyspark.streaming.tests import PySparkStreamingTestCase

from dte_app import DataTransformationEngine
from processors.processors import DummyProcessor
from socket_server import SocketServer

test_file_path_1 = 'test_resources/test1'
test_file_path_2 = 'test_resources/test2'


# TODO should split streaming and batch tests
class StreamingTest(PySparkStreamingTestCase):
    sc = None
    _hbaseTestingUtility = None

    # Kafka params
    timeout = 20  # seconds
    duration = 5

    test_topic_1 = 'topic1'
    test_topic_2 = 'topic2'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # kafka configuration
        kafka_test_utils_clz = cls.sc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
            .loadClass('org.apache.spark.streaming.kafka.KafkaTestUtils')
        cls._kafkaTestUtils = kafka_test_utils_clz.newInstance()
        cls._kafkaTestUtils.setup()
        ezk = cls._kafkaTestUtils.getClass().getDeclaredField('zookeeper')
        ezk.setAccessible(True)
        zk = ezk.get(cls._kafkaTestUtils).getClass().getDeclaredField('zookeeper')
        zk.setAccessible(True)
        zk.get(ezk.get(cls._kafkaTestUtils)).getServerCnxnFactory().setMaxClientCnxnsPerHost(100)

        # hbase configuration
        hbase_testing_utility_clz = cls.sc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
            .loadClass('org.apache.hadoop.hbase.HBaseTestingUtility')
        cls._hbaseTestingUtility = hbase_testing_utility_clz.newInstance()
        cls._hbaseTestingUtility.getConfiguration().setBoolean('hbase.table.sanity.checks', False)  # for thrift
        cls._hbaseTestingUtility.getConfiguration().set('hbase.zookeeper.property.clientPort',
                                                        cls._kafkaTestUtils.zkAddress().split(':')[1])

        cls._hbaseTestingUtility.startMiniDFSCluster(1)
        cls._hbaseTestingUtility.startMiniHBaseCluster(1, 1)
        # cls._hbaseTestingUtility.startMiniCluster()

        # thrift server configuration
        thrift_server_clz = cls.sc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
            .loadClass('org.apache.hadoop.hbase.thrift.ThriftServer')

        cArgs = cls.sc._gateway.new_array(cls.sc._jvm.java.lang.Class, 1)
        cArgs[0] = cls._hbaseTestingUtility.getConfiguration().getClass()
        iArgs = cls.sc._gateway.new_array(cls.sc._jvm.java.lang.Object, 1)
        iArgs[0] = cls._hbaseTestingUtility.getConfiguration()

        cls._thriftServer = thrift_server_clz \
            .getDeclaredConstructor(cArgs) \
            .newInstance(iArgs)

        tArgs = cls.sc._gateway.new_array(cls.sc._jvm.java.lang.String, 5)
        port = cls._hbaseTestingUtility.randomFreePort()
        cls.thrift_port = port
        tArgs[0] = "-port"
        tArgs[1] = str(port)
        tArgs[2] = "-infoport"
        info_port = cls._hbaseTestingUtility.randomFreePort()
        tArgs[3] = str(info_port)
        tArgs[4] = "start"

        mArgs = cls.sc._gateway.new_array(cls.sc._jvm.java.lang.Class, 1)
        mArgs[0] = tArgs.getClass()
        method = thrift_server_clz.getDeclaredMethod('doMain', mArgs)
        method.setAccessible(True)

        args = cls.sc._gateway.new_array(cls.sc._jvm.java.lang.Object, 1)
        args[0] = tArgs
        cls.thrift_server_thread = threading.Thread(target=method.invoke, args=[cls._thriftServer, args])
        cls.thrift_server_thread.setDaemon(True)
        cls.thrift_server_thread.start()
        time.sleep(5)

        cls._hbaseTestingUtility.getMiniHBaseCluster().waitForActiveAndReadyMaster(60000)

        # test topics
        cls._kafkaTestUtils.createTopic(cls.test_topic_1)
        cls._kafkaTestUtils.createTopic(cls.test_topic_2)
        cls._kafkaTestUtils.createTopic('test_topic1')
        cls._kafkaTestUtils.createTopic('test_topic2')

        # HBase configuration
        connection = happybase.Connection(port=cls.thrift_port)
        connection.create_table(
            DummyProcessor.TABLE_NAME,
            {'dummy': dict()}
        )
        connection.create_table(
            'test_' + DummyProcessor.TABLE_NAME,
            {'dummy': dict()}
        )

        # HDFS configuration (uses for co-processors)
        # dfs = cls._hbaseTestingUtility.getDFSCluster().getFileSystem()
        # hdfs = HDFileSystem(host='localhost',
        #                     port=dfs.getUri().getPort(),
        #                     pars={'dfs.client.read.shortcircuit': 'false'})
        #
        # hdfs.put('../../../../hbase-coprocessors/target/scala-2.10/hbase-coprocessors.jar',
        #          '/hbase-coprocessors.jar')

        # streaming engine test configuration
        cls.engine_config = {
            'socketServer.port': 4444,  # port of socket server

            'hadoop.dfs.url': 'hdfs://localhost:{}/'.format(cls._hbaseTestingUtility.getDFSCluster().getFileSystem()
                                                      .getUri().getPort()),
            'hadoop.conf': {
                'hbase.table.sanity.checks': False,
                'hbase.zookeeper.property.clientPort': cls._kafkaTestUtils.zkAddress().split(':')[1]
            },

            'zookeeper.zkQuorum': cls._kafkaTestUtils.zkAddress(),

            'kafka.groupId': 'test-streaming-consumer',
            'kafka.params': {'auto.offset.reset': 'largest'},

            'hbase.host': 'localhost',
            'hbase.port': cls.thrift_port,

            'processor.dir': 'processors',
            'processor.joinWindow': 20,
            'processor.poolExecutors': 1
        }

    @classmethod
    def tearDownClass(cls):
        if cls._hbaseTestingUtility is not None:
            # cls._hbaseTestingUtility.shutdownMiniHBaseCluster()
            # cls._hbaseTestingUtility.shutdownMiniDFSCluster()
            cls._hbaseTestingUtility.shutdownMiniCluster()

        if cls._kafkaTestUtils is not None:
            cls._kafkaTestUtils.teardown()
            cls._kafkaTestUtils = None

        super().tearDownClass()

    def setUp(self):
        super(StreamingTest, self).setUp()

    def tearDown(self):
        super(StreamingTest, self).tearDown()


    def test_streaming_mode(self):
        # self.sc.setLogLevel('DEBUG')
        try:
            os.remove('processors/test_processors.py')
        except Exception as e:
            pass  # it's Ok

        engine = DataTransformationEngine(self.sc, self.engine_config)
        engine.start()
        # wait up to 10s for the server to start
        time.sleep(10)

        test_1 = '{"id": "12345", "score": "100"}'
        test_2 = '{"id": "12345", "another_score": "200"}'

        self._kafkaTestUtils.sendMessages(self.test_topic_1, {test_1: 5})
        self._kafkaTestUtils.sendMessages(self.test_topic_2, {test_2: 5})

        time.sleep(5)
        connection = happybase.Connection(port=self.thrift_port)
        table = connection.table('dummy_table')
        row = table.row(b'12345', columns=[b'dummy:score'])
        print(row)
        self.assertEquals(row[b'dummy:score'], b'0.5')

        with open('processors/processors.py') as python_file, open('processors/test_processors.py',
                                                                   'w') as test_python_file:
            test_python_file \
                .write(python_file
                       .read()
                       .replace('DummyProcessor', 'TestDummyProcessor')
                       .replace('dummy_table', 'test_dummy_table')
                       .replace('topic1', 'test_topic1')
                       .replace('topic2', 'test_topic2')
                       .replace('/', '*'))

        with socket.create_connection(('localhost', self.engine_config['socketServer.port'])) as sock:
            sock.sendall(bytes(SocketServer.Messages.RESTART_STREAMING_SIGNAL, 'utf-8'))
            received = str(sock.recv(1024), 'utf-8')
            self.assertTrue(received == SocketServer.Messages.RESTART_STREAMING_RESPONSE)

        time.sleep(20)
        self._kafkaTestUtils.sendMessages('test_topic1', {test_1: 5})
        self._kafkaTestUtils.sendMessages('test_topic2', {test_2: 5})

        time.sleep(5)
        test_row = connection.table('test_dummy_table') \
            .row(b'12345', columns=[b'dummy:score'])

        self.assertEquals(test_row[b'dummy:score'], b'20000')

        engine.stop()
        os.remove('processors/test_processors.py')

    @unittest.skip('becouse')
    def test_batch_mode(self):
        self._hbaseTestingUtility.getMiniHBaseCluster().waitForActiveAndReadyMaster(60000)

        dfs = self._hbaseTestingUtility.getDFSCluster().getFileSystem()
        hdfs = HDFileSystem(host='localhost',
                            port=dfs.getUri().getPort(),
                            pars={'dfs.client.read.shortcircuit': 'false'})

        hdfs.put('test_resources/batch_test', '/batch_test')

        self.engine_config['mode'] = 'batch'
        engine = DataTransformationEngine(self.sc, self.engine_config)
        engine.start()
        # wait up to 10s for the server to start
        time.sleep(5)

        expected = b"{'table_name': 'dummy_table', 'column_family': 'dummy', 'row': '1', 'data': '1', 'column': " \
                   b"'count'}\n{'table_name': 'dummy_table', 'column_family': 'dummy', 'row': '2', 'data': '2', " \
                   b"'column': 'count'}\n{'table_name': 'dummy_table', 'column_family': 'dummy', 'row': '3', " \
                   b"'data': '3', 'column': 'count'}\n{'table_name': 'dummy_table', 'column_family': 'dummy', " \
                   b"'row': '4', 'data': '4', 'column': 'count'}\n{'table_name': 'dummy_table', 'column_family': " \
                   b"'dummy', 'row': '5', 'data': '5', 'column': 'count'}\n "

        with hdfs.open('/result/part-00000', replication=1) as f:
            self.assertTrue(f.read(), expected)


if __name__ == '__main__':
    unittest.main()
