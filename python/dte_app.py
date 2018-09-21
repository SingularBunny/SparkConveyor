import argparse
import threading

import time
from pyspark import SparkContext
from pyspark.ml import Pipeline, Transformer
from pyspark.streaming import StreamingContext
from yaml import load, Loader

from python.processing import get_subclasses, Processor, HasConfig, HasStreamingContext, HasStages
from python.socket_server import SocketServer, RequestHandler


class DataTransformationEngine:

    def __init__(self, spark_context, config):
        """
        :param SparkContext spark_context: SparkContext instance.
        :param dict config: configuration dictionary, contains: zkQuorum(str), groupId(str), topics(list).
        """

        self.spark_context = spark_context
        self.config = config
        self.spark_streaming_context = None
        self.socket_server = None
        self.socket_server_thread = None
        self.topics = None
        self.run_thread = None
        self.stop_event = threading.Event()
        self.restart_event = threading.Event()
        self.processors = None
        self.mode = config['engine.mode'] if 'engine.mode' in config else 'stream'

    def init(self):
        self.socket_server = SocketServer(('localhost', self.config['socketServer.port']),
                                          RequestHandler, self.stop_event, self.restart_event)
        self.socket_server_thread = threading.Thread(target=self.socket_server.serve_forever)

    def process(self):
        if self.mode == 'batch':

            self.processors = filter(lambda prc: issubclass(prc, Transformer)
                                     and issubclass(prc, Processor)
                                     and issubclass(prc, HasConfig)
                                     and not issubclass(prc, HasStreamingContext)
                                     and issubclass(prc, HasStages),
                                     get_subclasses(self.config['processor.dir'],
                                                    (Pipeline, Transformer, Processor, HasConfig)))

            for processor in self.processors:
                Pipeline(processor(self.config, self.spark_context, mode='batch').getStages())\
                    .fit(None).transform(None)

        elif self.mode == 'stream':
            while not self.stop_event.is_set():

                if self.spark_streaming_context is None:
                    # TODO move batch duration to config
                    self.spark_streaming_context = StreamingContext(self.spark_context, 0.5)

                    self.processors = filter(lambda prc: issubclass(prc, Transformer)
                                             and issubclass(prc, Processor)
                                             and issubclass(prc, HasConfig)
                                             and issubclass(prc, HasStreamingContext)
                                             and issubclass(prc, HasStages),
                                             get_subclasses(self.config['processor.dir'],
                                                            (Pipeline, Transformer, Processor, HasConfig,
                                                             HasStreamingContext)))

                    for processor in self.processors:
                        Pipeline(stages=processor(config=self.config, ssc=self.spark_streaming_context).getStages())\
                            .fit(None).transform(None)

                    self.spark_streaming_context.start()
                else:
                    if self.restart_event.is_set():
                        self.spark_streaming_context.stop(stopSparkContext=False, stopGraceFully=True)
                        self.spark_streaming_context.awaitTermination()
                        self.spark_streaming_context = None
                        self.restart_event.clear()
                    else:
                        time.sleep(0.1)
            else:
                self.spark_streaming_context.stop(stopSparkContext=False, stopGraceFully=True)
                self.spark_streaming_context.awaitTermination()
                self.spark_streaming_context = None

    def run(self):
        self.init()
        process_thread = threading.Thread(target=self.process)
        process_thread.start()

        # start socket server to wait termination
        if self.mode == 'stream':
            self.socket_server_thread.start()

        process_thread.join()
        self.clean_up_jvm()

    def clean_up_jvm(self):
        # Clean up in the JVM just in case there has been some issues in Python API
        try:
            jStreamingContextOption = StreamingContext._jvm.SparkContext.getActive()
            if jStreamingContextOption.nonEmpty():
                jStreamingContextOption.get().stop(False)
            jSparkContextOption = SparkContext._jvm.SparkContext.get()
            if jSparkContextOption.nonEmpty():
                jSparkContextOption.get().stop()
        except Exception:
            # TODO send info to Logger
            pass

    def start(self):
        self.run_thread = threading.Thread(target=self.run)
        self.run_thread.start()

    def stop(self, await_termination=False, timeout=None):
        self.stop_event.set()
        if not self.socket_server.is_shut_down():
            self.socket_server.shutdown()
        self.socket_server.server_close()
        if await_termination:
            self.run_thread.join(timeout)


if __name__ == '__main__':
    # parse command line args
    parser = argparse.ArgumentParser(description='Process custom scripts.')
    parser.add_argument('-c', '--config',
                        default='config.yml',
                        type=str,
                        help="Specify config file",
                        metavar="FILE",
                        dest='config')

    args = parser.parse_args()

    with open(args.config) as config_file:
        config = load(config_file, Loader=Loader)
        config.update(vars(args))

    sc = SparkContext(appName='Predictor Engine App')
    engine = DataTransformationEngine(sc, config)
    engine.run()
