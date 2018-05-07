from unittest import TestCase

from processors import DummyProcessor


class DynamicClassLoadingTest(TestCase):
    def test_dynamic_loading(self):
        processors = get_transformers("processors")
        self.assertTrue(filter(lambda prc: prc.__name__ in [DummyProcessor.__name__], processors))
