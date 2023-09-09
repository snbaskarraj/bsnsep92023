import json

from dateutil import parser

from src.com.iot.model.ConfigRuleDataModel import Config_Rule_Data_Model
from src.com.iot.service.Aggregator import Aggregator
from src.com.iot.service.AnomalyDetector import Anomaly_Detector
from src.com.iot.service.IProcessor import IProcessor
from src.com.iot.util import Database
from src.com.iot.util.Database import get_invalid_rule_value

Database.create_dynamodb_table("bsm_agg_data")
Database.create_dynamodb_table("bsm_alerts")


def aggregator_handler():
    from_date = parser.parse("2023-09-08 20:45:25")
    to_date = parser.parse("2023-09-08 20:47:32")
    aggregator: IProcessor = Aggregator(from_date, to_date)
    aggregator.process()


def anomaly_detector_handler():
    f = open("../../../anomaly_rules/config_rule.json", "r")
    config_rule = json.load(f)
    anomaly_data_rules = []
    for rule in config_rule:
        avg_max = rule["avg_max"] if "avg_max" in rule else -get_invalid_rule_value()
        avg_min = rule["avg_min"] if "avg_min" in rule else get_invalid_rule_value()
        anomaly_data_rules.append(
            Config_Rule_Data_Model(rule["id"], rule["rule_type"], rule["device_type"], avg_min, avg_max,
                                   rule["trigger_count"]))

    anomaly_detector: IProcessor = Anomaly_Detector(anomaly_data_rules)
    anomaly_detector.process()


# print("config_rules ", anomaly_data_rules)
interactive = False

if not interactive:
    aggregator_handler()
    anomaly_detector_handler()
while interactive:
    choice = int(input(
        "Enter \n 1 to run both aggregator and ANomally Detector\n\t 2 to run only Aggregator \n\t 3 to run only the Anomaly Detector \n\t 0 to quit"))
    if choice == 0:
        break
    elif choice == 1:
        aggregator_handler()
        anomaly_detector_handler()
    elif choice == 2:
        aggregator_handler()
    elif choice == 3:
        anomaly_detector_handler()
