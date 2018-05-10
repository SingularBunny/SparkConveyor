import collections
import functools
import importlib
import inspect
import traceback
from abc import abstractmethod, ABCMeta

import os

from pyspark import SparkContext
from pyspark.ml import Transformer, Pipeline
from pyspark.ml.param import Params, Param
from pyspark.ml.util import keyword_only
from pyspark.mllib.common import inherit_doc


def get_subclasses(dir, parent_class):
    transformers = []
    for filename in os.listdir(dir):
        if filename[0] != '_' and filename.split('.')[-1] in ('py', 'pyw'):
            modulename = filename.split('.')[0]
            spec = importlib.util.spec_from_file_location(modulename,
                                                          (dir
                                                           if dir.endswith('/')
                                                           else dir + '/') + filename)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            for _, transformer in inspect.getmembers(module,
                                                     lambda member:
                                                     inspect.isclass(member)
                                                     and not inspect.isabstract(member)
                                                     and issubclass(member, parent_class)):
                transformers.append(transformer)
    return transformers


def process_partition(class_name, processors_dir, partition, **kwargs):
    """
    Process partition.

    :param class_name: Class name of processor to dynamic loading.
    :param processors_dir: directory where processors are placed.
    :param partition: dataset partition.
    :param kwargs: constructor arguments of processor.
    :return: transformed partition.
    """
    processor = next(filter(lambda proc: class_name == proc.__name__,
                            get_subclasses(processors_dir, Processor)))(**kwargs)

    for row in partition:
        try:
            yield processor.process(row)
        except Exception as e:
            print(traceback.format_exc())
            print(e)


def process_row(class_name, processors_dir, row, **kwargs):
    """
    Process single row.

    :param class_name: Class name of processor to dynamic loading.
    :param processors_dir: directory where processors are placed.
    :param row: row of dataset.
    :param kwargs: constructor arguments of processor.
    :return: transformed row.
    """
    process_partition(class_name, processors_dir, [row], **kwargs).pop()


@inherit_doc
class Processor(Params):
    """
    Base class for processors that compute attributes.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def process(self, data):
        """
        Process the output with optional parameters.

        :param tuple or list data: a data that contains row or rows of data
        :return: transformed row or list of rows.
        """
        raise NotImplementedError()


class HasConfig(Params):
    """
    Mixin for param config: configuration dict.
    """

    # a placeholder to make it appear in the generated doc
    config = Param(Params._dummy(), 'config', 'configuration dict.')

    def __init__(self):
        super(HasConfig, self).__init__()
        #: param for config
        self.config = Param(self, 'config', 'configuration dict.')
        self._setDefault(config={})

    def setConfig(self, value):
        """
        Sets the value of :py:attr:`config`.
        """
        self._paramMap[self.config] = value
        return self

    def getConfig(self):
        """
        Gets the value of config or its default value.
        """
        return self.getOrDefault(self.config)


class HasSparkContext(Params):
    """
    Mixin for param sc: Spark context.
    """

    # a placeholder to make it appear in the generated doc
    ssc = Param(Params._dummy(), 'sc', 'Spark context.')

    def __init__(self):
        super(HasSparkContext, self).__init__()
        #: param for Spark context
        self.sc = Param(self, 'sc', 'Spark context.')
        self._setDefault(sc=SparkContext.getOrCreate())

    def setSparkContext(self, value):
        """
        Sets the value of :py:attr:`sc`.
        """
        self._paramMap[self.sc] = value
        return self

    def getSparkContext(self):
        """
        Gets the value of spark context or its default value.
        """
        return self.getOrDefault(self.sc)


class HasSqlContext(HasSparkContext):
    """
    Mixin for param sql_context: SQL context.
    """

    # a placeholder to make it appear in the generated doc
    ssc = Param(Params._dummy(), 'sql_context', 'SQL context.')

    def __init__(self):
        super(HasSqlContext, self).__init__()
        #: param for SQL context
        self.sql_context = Param(self, 'sql_context', 'SQL context.')
        self._setDefault(sql_context=SparkContext.getOrCreate(self.sc))

    def setSqlContext(self, value):
        """
        Sets the value of :py:attr:`sql_context`.
        """
        self._paramMap[self.sql_context] = value
        return self

    def getSqlContext(self):
        """
        Gets the value of SQL context or its default value.
        """
        return self.getOrDefault(self.sql_context)


class HasStreamingContext(Params):
    """
    Mixin for param ssc: Spark streaming context.
    """

    # a placeholder to make it appear in the generated doc
    ssc = Param(Params._dummy(), 'ssc', 'Spark streaming context.')

    def __init__(self):
        super(HasStreamingContext, self).__init__()
        #: param for Spark streaming context
        self.ssc = Param(self, 'ssc', 'Spark streaming context.')

    def setStreamingContext(self, value):
        """
        Sets the value of :py:attr:`ssc`.
        """
        self._paramMap[self.ssc] = value
        return self

    def getStreamingContext(self):
        """
        Gets the value of StreamingContext or its default value.
        """
        return self.getOrDefault(self.ssc)


class HasFileParams(Params):
    """
    Mixin for file params: Spark streaming context.
    """

    # a placeholder to make it appear in the generated doc
    file_path = Param(Params._dummy(), "file_path", "path to file should be read.")
    format = Param(Params._dummy(), "format", "format of file: text, csv, parquet, avro, etc.")
    options = Param(Params._dummy(), "options", "options of file reading or writing as dict.")

    def __init__(self):
        super(HasFileParams, self).__init__()
        #: path to file should be read.
        self.file_path = Param(self, "file_path", "path to file should be read.")
        #: format of file: text, csv, parquet, avro, etc.
        self.format = Param(self, "format", "format of file: text, csv, parquet, avro, etc.")
        #: options of file reading or writing as dict.
        self.options = Param(self, "options", "options of file reading or writing as dict.")

    def setFilePath(self, value):
        """
        Sets the value of :py:attr:`file_path`.
        """
        self._paramMap[self.file_path] = value
        return self

    def getFilePath(self):
        """
        Gets the value of file_path or its default value.
        """
        return self.getOrDefault(self.file_path)

    def setFormat(self, value):
        """
        Sets the value of :py:attr:`format`.
        """
        self._paramMap[self.format] = value
        return self

    def getFormat(self):
        """
        Gets the value of format or its default value.
        """
        return self.getOrDefault(self.format)

    def setOptions(self, value):
        """
        Sets the value of :py:attr:`options`.
        """
        self._paramMap[self.options] = value
        return self

    def getOptions(self):
        """
        Gets the value of options or its default value.
        """
        return self.getOrDefault(self.options)


class HasStages(Params):
    """
    Mixin for param stages: pipeline stages
    """

    # a placeholder to make it appear in the generated doc
    stages = Param(Params._dummy(), "stages", "pipeline stages")

    def __init__(self):
        super(HasStages, self).__init__()
        #: Param for pipeline stages.
        self.stages = Param(self, "stages", "pipeline stages")

    def setStages(self, value):
        """
        Set pipeline stages.

        :param value: a list of transformers or estimators
        :return: the pipeline instance
        """
        self._paramMap[self.stages] = value
        return self

    def getStages(self):
        """
        Get pipeline stages.
        """
        return self.getOrDefault(self.stages)


@inherit_doc
class BaseTextFileTransformer(Transformer, Processor, HasConfig, HasStages, metaclass=ABCMeta):
    """
    Example transformer.
    """

    @keyword_only
    def __init__(self, config=None, stages=None):
        super(BaseTextFileTransformer, self).__init__()
        if config is None:
            config = {}

        if stages is None:
            stages = []

        kwargs = self.__init__._input_kwargs

        self._set(**kwargs)

    def _transform(self, dataset):
        processors_dir = self.getConfig()['processor.dir']
        class_name = self.__class__.__name__
        kwargs = {'config': self.getConfig()}

        def proc_partition(partition):
            return process_partition(class_name, processors_dir, partition, **kwargs)

        return dataset \
            .mapPartitions(proc_partition)

    @abstractmethod
    def process(self, data):
        raise NotImplementedError()


@inherit_doc
class BaseStreamProcessor(Transformer, Processor, HasConfig, HasStreamingContext, HasStages, metaclass=ABCMeta):
    """
    Example transformer.
    """

    @keyword_only
    def __init__(self, config=None, ssc=None, stages=None):
        super(BaseStreamProcessor, self).__init__()
        if config is None:
            config = {}

        if stages is None:
            stages = []

        kwargs = self.__init__._input_kwargs
        self._set(**kwargs)

    def _transform(self, dataset):
        if not isinstance(dataset, collections.Iterable):
            dataset = [].append(dataset)

        processors_dir = self.getConfig()['processor.dir']
        class_name = self.__class__.__name__
        kwargs = {'config': self.getConfig()}

        def proc_partition(partition):
            return process_partition(class_name, processors_dir, partition, **kwargs)

        return self.make_joined_stream(*dataset) \
            .mapPartitions(proc_partition)

    def make_joined_stream(self, *args):
        """
        Join logic.

        :param kwargs: (str name, DStream stream) pairs.
        :return: single joined stream.
        """
        if len(args) == 1:
            # single stream
            return list(args).pop()
        else:
            # joined stream
            return functools \
                .reduce(lambda stream1, stream2: stream1
                        .join(stream2), args)

    @abstractmethod
    def process(self, data):
        raise NotImplementedError()
