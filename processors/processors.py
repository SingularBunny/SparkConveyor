import json

from pyspark.ml.util import keyword_only

from processing import BaseTextFileTransformer, BaseStreamProcessor
from receivers import HbaseReceiver, FileReceiver
from sources import KafkaStreamSource, FileSource, CoProcessorSource


class DummyProcessor(BaseStreamProcessor):
    TABLE_NAME = 'dummy_table'
    COLUMN_FAMILY = 'dummy'
    COLUMN = 'score'
    TOPIC1 = 'topic1'
    TOPIC2 = 'topic2'

    @keyword_only
    def __init__(self, config=None, ssc=None):
        super(DummyProcessor, self).__init__(config=config, ssc=ssc)
        source1 = KafkaStreamSource(config=config, ssc=ssc, topic=self.TOPIC1)
        source2 = KafkaStreamSource(config=config, ssc=ssc, topic=self.TOPIC2)
        receiver = HbaseReceiver(config=config, table_name=self.TABLE_NAME)
        self.setStages([source1, source2, self, receiver])

    def _transform(self, dataset):
        dataset = [stream.map(json.loads).map(lambda row: (row['id'], row)) for stream in dataset]
        return super()._transform(dataset)

    def process(self, row):
        if len(row) != 0:
            key = row[0]
            dict1 = row[1][0]
            dict2 = row[1][1]

            total_score = int(dict1['score']) / int(dict2['another_score'])

            result = {'table_name': self.TABLE_NAME,
                      'row': key,
                      'column_family': self.COLUMN_FAMILY,
                      'column': self.COLUMN,
                      'data': total_score}
            return result


class CoPrcProcessor(BaseStreamProcessor):
    TABLE_NAME = 'dummy_table'
    COLUMN_FAMILY = 'dummy'
    COLUMN = 'score'
    TOPIC = 'topic'
    TABLE_NAME = ''
    HDFS_PATH = '/'

    @keyword_only
    def __init__(self, config=None, ssc=None):
        super(CoPrcProcessor, self).__init__(config=config, ssc=ssc)
        source = CoProcessorSource(config=config, ssc=ssc, topic=self.TOPIC)
        receiver = HbaseReceiver(config=config, table_name=self.TABLE_NAME)
        self.setStages([source, self, receiver])

    def _transform(self, dataset):
        dataset = [stream.map(json.loads).map(lambda row: (row['id'], row)) for stream in dataset]
        return super()._transform(dataset)

    def process(self, row):
        if len(row) != 0:
            key = row[0]
            dict1 = row[1][0]
            dict2 = row[1][1]

            total_score = int(dict1['score']) / int(dict2['another_score'])

            result = {'table_name': self.TABLE_NAME,
                      'row': key,
                      'column_family': self.COLUMN_FAMILY,
                      'column': self.COLUMN,
                      'data': total_score}
            return result


class TextProcessor(BaseTextFileTransformer):
    TABLE_NAME = 'dummy_table'
    COLUMN_FAMILY = 'dummy'
    COLUMN = 'count'
    FILE_SOURCE = 'test_resources/batch_test'
    FILE_DEST = 'result'
    @keyword_only
    def __init__(self, config=None):
        super(TextProcessor, self).__init__(config=config)
        source = FileSource(config=config, file_path=self.FILE_SOURCE)
        receiver = FileReceiver(config=config, file_path=self.FILE_DEST)
        self.setStages([source, self, receiver])

    def process(self, row):
        return {'table_name': self.TABLE_NAME,
                'row': row,
                'column_family': self.COLUMN_FAMILY,
                'column': self.COLUMN,
                'data': row}
