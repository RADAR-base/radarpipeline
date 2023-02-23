import unittest
from radarpipeline.features import FeatureGroup, Feature


class TestFeature(unittest.TestCase):
    class MockFeature(Feature):
        def preprocess(self, data):
            pass

        def calculate(self, data):
            pass

    def setUp(self):
        self.mockfeature = self.MockFeature(name='mock feature',
                                            description='mock feature description',
                                            required_input_data=['mock data'])

    def test_get_required_data(self):
        self.assertEqual(self.mockfeature.get_required_data(), ['mock data'])
