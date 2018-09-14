from abc import abstractmethod, ABCMeta

from pyspark.ml import Transformer
from pyspark.ml.param import Params, Param
from pyspark.ml.util import keyword_only
from pyspark.mllib.common import inherit_doc
from pyspark.streaming.kafka import KafkaUtils

from python.processing import HasConfig, HasStreamingContext, HasSqlContext, HasFileParams, HasKafkaSource


@inherit_doc
class Source(Params):
    """
    Base class for processors that compute attributes.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_source(self):
        raise NotImplementedError()


@inherit_doc
class KafkaStreamSource(Transformer, Source, HasConfig, HasStreamingContext, HasKafkaSource):

    @keyword_only
    def __init__(self, config=None, ssc=None, topic=None, num_partitions=1):
        """
        Kafka stream source.

        :param config: configuration dict.
        :param ssc: Spark streaming context.
        :param topic: Kafka topic name.
        :param num_partitions: number of partitions ny topic.
        """
        super(KafkaStreamSource, self).__init__()
        if config is None:
            config = {}

        self._setDefault(num_partitions=1)

        kwargs = self.__init__._input_kwargs
        self._set(**kwargs)

    def _transform(self, dataset):
        if dataset is None:
            dataset = []
        dataset.append(self.get_source().map(lambda x: x[1])
                       .window(self.getOrDefault(self.config)['processor.joinWindow']))

        return dataset

    def get_source(self):
        """
        Returns Kafka stream
        """
        config = self.getConfig()
        return KafkaUtils \
            .createStream(self.getStreamingContext(),
                          config['zookeeper.zkQuorum'],
                          config['kafka.groupId'],
                          {self.getOrDefault(self.topic): self.getOrDefault(self.num_partitions)},
                          config['kafka.params'])


@inherit_doc
class CoProcessorSource(Transformer, Source, HasConfig, HasStreamingContext, HasKafkaSource):

    @keyword_only
    def __init__(self, config=None, ssc=None, topic=None, num_partitions=1, table_name=None, hdfs_path=None):
        """
        Hbase co-processor source trough Kafka.

        :param config: configuration dict.
        :param ssc: Spark streaming context.
        :param topic: Kafka topic name.
        :param num_partitions: number of partitions ny topic.
        :param table_name: table where co-processor works.
        :param hdfs_path:
        """
        super(CoProcessorSource, self).__init__()
        self.table_name = Param(self, "table_name", "table where co-processor works.")
        self.hdfs_path = Param(self, "hdfs_path", "number of partitions by topic.")

        self._setDefault(num_partitions=1)

        kwargs = self.__init__._input_kwargs
        self._set(**kwargs)

        if ssc:
            self._pl = self.getStreamingContext()._jvm.ru.homecredit.smartdata.coprocessors.PutListener
            conf = self._pl.getConfiguration()
            for key, value in config['hadoop.conf'].items():
                conf.set(key, str(value))
            self._pl.deployMe(table_name, config['hadoop.dfs.url'] + str(hdfs_path))

    def _transform(self, dataset):
        if dataset is None:
            dataset = []
        dataset.append(self.get_source().map(lambda x: x[1])
                       .window(self.getOrDefault(self.config)['processor.joinWindow']))

        return dataset

    def get_source(self):
        """
        Returns Kafka stream
        """
        config = self.getConfig()
        return KafkaUtils \
            .createStream(self.getStreamingContext(),
                          config['zookeeper.zkQuorum'],
                          config['kafka.groupId'],
                          {self.getOrDefault(self.topic): self.getOrDefault(self.num_partitions)},
                          config['kafka.params'])

@inherit_doc
class FileSource(Transformer, Source, HasSqlContext, HasFileParams):
    @keyword_only
    def __init__(self, config=None, sc=None, file_path=None, options=None, format='text'):
        super().__init__()
        if options is None:
            options = {}

        kwargs = self.__init__._input_kwargs
        self._set(**kwargs)

    def _transform(self, dataset):
        return self.get_source()

    def get_source(self):
        if self.getFormat() == 'text':
            return self.getSparkContext().textFile(self.getFilePath())
        else:
            self.getSqlContext() \
                .read \
                .format(self.getFormat()) \
                .options(**self.getOptions()) \
                .load(self.getFilePath())
