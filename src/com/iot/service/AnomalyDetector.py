#import parser

from dateutil import parser

from src.com.iot.model.Anomaly_Data_Model import Anomaly_Data_Model
from src.com.iot.model.ConfigRuleDataModel import Config_Rule_Data_Model
from src.com.iot.service.IProcessor import IProcessor
from src.com.iot.util.Database import Database, marshall_data, get_registered_devices, get_invalid_rule_value


class Anomaly_Detector(IProcessor):

    def __init__(self, config_rule=None):
        if config_rule is None:
            config_rule = []

        self.anomaly_rule = config_rule
        self.existing_device_ids = get_registered_devices()
        self._database = Database("bsm_alerts")

    def process(self):


        for device_id in self.existing_device_ids:
            # read data from aggregate table
            aggregated_data: [] = self._database.get_data_from_table("bsm_agg_data",device_id)
            print("Processing rules for device {0}".format(device_id))
            self.detect_anomaly(aggregated_data, self.anomaly_rule, device_id)

    def detect_anomaly(self, aggregated_data, anomaly_rule_list, deviceid):
        if len(aggregated_data) > 0 and len(anomaly_rule_list) > 0:
            # filter data for the given device id
            data = list(filter(lambda item: item["deviceid"] == deviceid, aggregated_data))
            # sort the data by timestamp
            data.sort(key=lambda x: parser.parse(x["timestamp"]))

            for anomaly_rule in anomaly_rule_list:
                # filter data by the sensor/datatype type
                sensor_data = list(filter(lambda item: item["device_type"] == anomaly_rule.device_type, data))
                self.apply_and_validate_anomaly(anomaly_rule, sensor_data)

            # print("Aggregated data ", aggregated_data)  # print("Rules ", self.anomaly_rule)

    def apply_and_validate_anomaly(self, anomaly_rule: Config_Rule_Data_Model, items):
        trigger_count = anomaly_rule.trigger_count
        front_tracker = 0

        # use a sliding window og size trigger _count
        while front_tracker < len(items):
            end_tracker = front_tracker + trigger_count - 1
            if end_tracker >= len(items):
                end_tracker = len(items) - 1
            # apply a given anomaly rule to the data set
            self.detect_process_anomaly(anomaly_rule, items, front_tracker, end_tracker)

            front_tracker += 1

    def detect_process_anomaly(self, anomaly_rule: Config_Rule_Data_Model, items, start_index, end_index):

        count = start_index

        start_time = items[start_index]["start_time"]
        end_time = items[end_index]["end_time"]

        anomaly_detected = True
        while count <= end_index:

            average = items[start_index]["average"]

            # valid values indicate no anomaly hence skip processing other data in the current sliding window

            # OR clause in rule
            if (anomaly_rule.avg_max == -get_invalid_rule_value() and average >= anomaly_rule.avg_min) or (anomaly_rule.avg_min == get_invalid_rule_value() and average <= anomaly_rule.avg_max):
                anomaly_detected = False
                break
            elif anomaly_rule.avg_min <= average <= anomaly_rule.avg_max: # and clause
                anomaly_detected = False
                break

            count += 1

        # if anomaly_rule.min_or_max == Conditions_Enum.any:
        #     if avg_minimum < anomaly_rule.avg_min:
        #         breach_type = "min"
        #     elif avg_maximum > anomaly_rule.avg_max:
        #         breach_type = "max"
        # else:
        #     if avg_minimum < anomaly_rule.avg_min and avg_maximum < anomaly_rule.avg_max:
        #         breach_type = "both"

        if anomaly_detected:
            self.insert_anomaly(items[end_index]["deviceid"], start_time, end_time, items[start_index]["average"],
                                anomaly_rule)

    def insert_anomaly(self, deviceid, start_time, end_time, average_value, rule):
        anomaly_model = Anomaly_Data_Model(deviceid, start_time, end_time, average_value, rule)
        print("Alert for device_id {0} on rule {1} starting at {2} with breach type {3}".format(deviceid, rule.id,start_time, rule.rule_type))
        self._database.insert_data(marshall_data(anomaly_model))


