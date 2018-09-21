from unittest import TestCase

from python.processing import get_subclasses, Processor
from python.processors.processors import DummyProcessor


class DynamicClassLoadingTest(TestCase):
    def test_dynamic_loading(self):
        processors = get_subclasses('./', Processor)
        self.assertTrue(filter(lambda prc: prc.__name__ in [DummyProcessor.__name__], processors))
