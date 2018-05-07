import json
from abc import ABC, abstractmethod
from base64 import b64decode
from xml.etree import ElementTree


class TopicHandler(ABC):
    """
    Base class for all Topic Handlers. Each Topic Handler should
    provide info about own topic and implement method row_to_kv
    that transform row(str) to (k(str),v(str)) pair.
    Key is being used for join multiple topics.
    """
    @abstractmethod
    def topic_name(self):
        """
        Get topic name.
        :return: topic name
        :type: str
        """
        raise NotImplementedError()

    @abstractmethod
    def row_to_kv(self, row):
        """
        Row (str) to (k(str), v(str)) transform function.
        :param row: row from stream
        :type row: str
        :return: (k(str), v(str)) pair
        """
        raise NotImplementedError()

    def handle(self, stream):
        return stream.map(self.row_to_kv)