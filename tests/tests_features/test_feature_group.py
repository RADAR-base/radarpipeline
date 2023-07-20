import unittest
from radarpipeline.features import FeatureGroup, Feature


class TestFeature(unittest.TestCase):
    class MockFeature(Feature):
        def __init__(self):
            self.name = "Mock Feature"
            self.description = "Mock Feature Description"
            self.required_input_data = ["mock_data_1"]

        def preprocess(self, data):
            pass

        def calculate(self, data):
            pass

    class MockFeature2(Feature):
        def __init__(self):
            self.name = "Mock Feature 2"
            self.description = "Mock Feature Description 2"
            self.required_input_data = ["mock_data_2"]

        def preprocess(self, data):
            pass

        def calculate(self, data):
            pass

    class MockFeatureGroup(FeatureGroup):
        def preprocess(self, data):
            pass

    def setUp(self):
        self.mockfeaturegroup = self.MockFeatureGroup(name='mock feature group',
                                                      description=("mock feature "
                                                                   "group description"),
                                                      features=[self.MockFeature,
                                                                self.MockFeature2])

    def test_get_required_data(self):
        self.assertCountEqual(self.mockfeaturegroup.get_required_data(),
                              ['mock_data_1', 'mock_data_2'])
