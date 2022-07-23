import numpy as np

from radarpipeline.features import Feature, FeatureGroup


class MedianHeartRate(Feature):
    def __init__(self):
        self.name = "Median Heart rate while sleeping"
        self.description = "Median Heart rate during different sleep stages"
        self.required_input_data = [
            "connect_fitbit_intraday_heart_rate",
            "connect_fitbit_sleep_stages",
        ]
        super().__init__(self.name, self.description, self.required_input_data)

    def calculate(self, data):
        # Get the heart rate data
        hr_data = data.get_combined_data_by_variable(
            "connect_fitbit_intraday_heart_rate"
        )
        hr_data["value.date"] = hr_data["value.time"].dt.date
        hr_data_median = (
            hr_data.groupby(["key.userId", "value.date"])["value.heartRate"]
            .median()
            .reset_index()
        )
        return hr_data_median["value.heartRate"]


class SdHeartRate(Feature):
    def __init__(self):
        self.name = "Heart rate SD while sleeping"
        self.description = "Heart rate SD during different sleep stages"
        self.required_input_data = [
            "connect_fitbit_intraday_heart_rate",
            "connect_fitbit_sleep_stages",
        ]
        super().__init__(self.name, self.description, self.required_input_data)

    def calculate(self, data):
        # Get the heart rate data
        hr_data = data.get_combined_data_by_variable(
            "connect_fitbit_intraday_heart_rate"
        )
        hr_data["value.date"] = hr_data["value.time"].dt.date
        hr_data_std = (
            hr_data.groupby(["key.userId", "value.date"])["value.heartRate"]
            .std()
            .reset_index()
        )
        return hr_data_std["value.heartRate"]


class HeartRateSleep(FeatureGroup):
    def __init__(self):
        self.name = "heart-rate-sleep-features"
        self.description = "HeartRate and Sleep feature"
        self.features = [MedianHeartRate(), SdHeartRate()]
        super().__init__(
            self.name, description=self.description, features=self.features
        )

    def compute_features(self, data):
        self.computed_features = self.get_all_features(data)
        return self.computed_features

    def preprocess(self):
        pass
