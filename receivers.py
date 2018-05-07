from abc import ABC, abstractmethod, ABCMeta
from argparse import ArgumentTypeError

import happybase
from pyspark import RDD
from pyspark.ml import Transformer
from pyspark.ml.param import Params, Param
from pyspark.ml.util import keyword_only
from pyspark.mllib.common import inherit_doc
from pyspark.sql import DataFrame

from processing import HasConfig, HasFileParams


@inherit_doc
class Receiver(Params):
    """
    Base class for receivers that compute attributes.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def receive(self, dataset):
        raise NotImplementedError()


@inherit_doc
class HbaseReceiver(Transformer, Receiver, HasConfig):

    @keyword_only
    def __init__(self, config=None, table_name=None):
        super(HbaseReceiver, self).__init__()

        self.table_name = Param(self, "table_name", "name of receive table.")

        kwargs = self.__init__._input_kwargs
        self._set(**kwargs)

    def _transform(self, dataset):
        self.receive(dataset)
        return dataset

    def receive(self, dataset):
        host = self.getConfig()['hbase.host']
        port = self.getConfig()['hbase.port']
        table_name = self.getOrDefault(self.table_name)

        def batch(partition):
            connection = happybase.Connection(host, port)
            table = connection.table(table_name)
            b = table.batch()
            for row in partition:
                b.put(bytes(row['row'], 'utf-8'),
                      {bytes(row['column_family'] + ':' + row['column'], 'utf-8'):
                           bytes(str(row['data']), 'utf-8')})

            b.send()

        dataset.foreachRDD(lambda rdd: rdd.foreachPartition(batch))


@inherit_doc
class FileReceiver(Transformer, Receiver, HasConfig, HasFileParams):

    @keyword_only
    def __init__(self, config=None, file_path=None, format='text', options=None):
        super(FileReceiver, self).__init__()
        if options is None:
            options = {}

    def _transform(self, dataset):
        self.receive(dataset)
        return dataset

    def receive(self, dataset):
        if isinstance(dataset, RDD):
            dataset \
                .repartition(1) \
                .saveAsTextFile(self.getFilePath())
        elif isinstance(dataset, DataFrame):
            dataset.write \
                .repartition(1) \
                .format(self.getFormat()) \
                .options(**self.getOptions()) \
                .save(self.getFilePath())
        else:
            raise ArgumentTypeError('Argument type {} is not supported'.format(dataset.__class__.__name__))
