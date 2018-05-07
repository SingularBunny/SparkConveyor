from pyspark.ml import Pipeline
from pyspark.ml.util import keyword_only

from processing import BaseTextFileTransformer, BaseStreamProcessor, HasStreamingContext
from receivers import HbaseReceiver, FileReceiver
from sources import KafkaStreamSource, FileSource


class DummyProcessor(Pipeline, BaseStreamProcessor, HasStreamingContext):
    TABLE_NAME = 'dummy_table'
    COLUMN_FAMILY = 'dummy'
    COLUMN = 'score'
    TOPIC1 = 'topic1'
    TOPIC2 = 'topic2f'

    @keyword_only
    def __init__(self, config=None, ssc=None):
        super(DummyProcessor, self).__init__(config=config)
        source1 = KafkaStreamSource(config, ssc, self.TOPIC1)
        source2 = KafkaStreamSource(config, ssc, self.TOPIC2)
        receiver = HbaseReceiver(config, self.TABLE_NAME)
        self.setStages([source1, source2, self, receiver])

        kwargs = self.__init__._input_kwargs
        self._set(**kwargs)

    def process(self, row):
        if len(row) != 0:
            key = row[0]
            topics_dict = dict(zip(self.topics, row[1]))
            total_score = None

            result = {'table_name': self.TABLE_NAME,
                      'row': key,
                      'column_family': self.COLUMN_FAMILY,
                      'column': self.COLUMN,
                      'data': total_score}

            return result


class TextProcessor(Pipeline, BaseTextFileTransformer):
    TABLE_NAME = 'dummy_table'
    COLUMN_FAMILY = 'dummy'
    COLUMN = 'count'
    FILE_SOURCE = 'resources/batch_test'
    FILE_DEST = 'result'
    @keyword_only
    def __init__(self, config=None):
        super(TextProcessor, self).__init__(config=config)
        source = FileSource(config, file_path=self.FILE_SOURCE)
        receiver = FileReceiver(config, self.FILE_DEST)
        self.setStages([source, self, receiver])

    def process(self, row):
        return {'table_name': self.TABLE_NAME,
                'row': row,
                'column_family': self.COLUMN_FAMILY,
                'column': self.COLUMN,
                'data': row}
