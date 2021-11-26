import pandas as pd

class RadarData():
    def __init__(self):
        self.data = {}

    def add_data(self, key: str, value:pd.DataFrame):
        self.data[key] = value

    def get_data(self, key: str):
        return self.data[key]

    def get_all_data(self):
        return self.data

    def get_data_keys(self):
        return list(self.data.keys())

    def get_data_size(self):
        return len(self.data)

    def get_data_keys_as_list(self):
        return list(self.data.keys())