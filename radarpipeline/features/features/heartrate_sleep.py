from .. import Feature, FeatureGroup
import numpy as np

class MedianHeartRate(Feature):
    def __init__(self):
        self.name = "Median Heart rate while spleeping"
        self.description = "Median Heart rate during different sleep stages"
        self.required_input_data = ["connect_fitbit_intraday_heart_rate", "connect_fitbit_sleep_stages"]
        super().__init__(self.name, self.description, self.required_input_data)

    def calculate(self):
        return np.median(self.data.heart_rate)

class SdHeartRate(Feature):
    def __init__(self):
        self.name = "Heart rate SD while spleeping"
        self.description = "Heart rate SD during different sleep stages"
        self.required_input_data = ["connect_fitbit_intraday_heart_rate", "connect_fitbit_sleep_stages"]
        super().__init__(self.name, self.description, self.required_input_data)

    def calculate(self):
        return np.std(self.data.heart_rate)

class HeartRateSleep(FeatureGroup):
    def __init__(self):
        self.name = "heart-rate-sleep-features"
        self.description = "HeartRate and Sleep feature"
        self.features = [MedianHeartRate(), SdHeartRate()]
        super().__init__(self.name, description=self.description, features=self.features)

    def compute_features(self):
        pass

    def preprocess(self):
        pass




