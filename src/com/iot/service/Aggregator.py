from datetime import timedelta
from typing import List
from dateutil import parser
from src.com.iot.model.AggregatedDataModel import Aggregated_Data_Model
from src.com.iot.service.IProcessor import IProcessor
from src.com.iot.util.Database import Database, marshall_data, get_registered_devices

def format_decimal_digits(value):
    return '{0:.3g}'.format(value)

def is_date_in_range(value, from_date, to_date):
    parsed_date = parser.parse(value)
    return from_date < parsed_date <= to_date

class Aggregator(IProcessor):
    def __init__(self, from_date, to_date, time_interval="60"):
        self._time_interval = time_interval
        self._from_date = from_date
        self._to_date = to_date
        self._database = Database("bsm_agg_data")
        self.existing_device_ids = get_registered_devices()

    @property
    def databse(self):
        return self._database

    def process(self):
        # aggregate data by device id
        for device_id in self.existing_device_ids:
            raw_data_set: List = self.databse.get_data_from_table("bsm_data", device_id)
            print("Aggregating data for device {0}".format(device_id))
            self.aggregate_device_data_for_device(device_id, raw_data_set)

    def aggregate_device_data_for_device(self, deviceid, items: List):
        # filter the data by device id and timerange
        device_data = list(
            filter(lambda item: is_date_in_range(item["timestamp"], self._from_date, self._to_date), items))

        # sort the data by timerange
        device_data.sort(key=lambda x: parser.parse(x["timestamp"]))

        start_time = self._from_date
        sensors = ["HeartRate", "SPO2", "Temperature"]

        aggregated_count = 0  # New counter introduced here

        while start_time < self._to_date:
            # data in the range of 1 minute from starting point
            end_time = start_time + timedelta(minutes=1)

            # aggregate data by start and end date for each sensor
            for device_type in sensors:
                # get the current sensor data per minute
                filtered_data_set: list = list(filter(
                    lambda item: item["datatype"] == device_type and is_date_in_range(item["timestamp"], start_time,
                                                                                      end_time), device_data))
                if len(filtered_data_set) > 0:
                    print("aggregate data from {0} and {1}, data = {2}".format(start_time, end_time,
                                                                               filtered_data_set))
                    # calculate minimum, maximum and average
                    minm = float('inf')
                    maxm = float('-inf')
                    total = 0
                    for item in filtered_data_set:
                        if "value" in item:  # Check if "value" key exists
                            value = float(item["value"])
                            if value > maxm:
                                maxm = value
                            if value < minm:
                                minm = value
                            total += value
                        else:
                            print("Warning: item without 'value' key found:", item)

                    avg = total / len(filtered_data_set)

                    # insert the aggregated data into dynamodb tbl
                    agdm = Aggregated_Data_Model(deviceid, device_type, format_decimal_digits(avg),
                                                 format_decimal_digits(minm), format_decimal_digits(maxm),
                                                 start_time, end_time)

                    self._database.insert_data(marshall_data(agdm))
                    aggregated_count += 1  # Increment the counter

            start_time = end_time

        # Updated the print statements to use aggregated_count
        print("Data Count for all sensors ", len(device_data), " aggregated data count ", aggregated_count)
        print("End aggregating data for :: ", deviceid)
        print("deviceId ", deviceid)
        print("device_data ", device_data)
