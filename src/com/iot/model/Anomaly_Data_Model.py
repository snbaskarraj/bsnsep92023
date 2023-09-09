import datetime
import json
import uuid
from decimal import Decimal

from src.com.iot.model.ConfigRuleDataModel import Config_Rule_Data_Model


class Anomaly_Data_Model:
    def __init__(self, deviceid, start_time, end_time, average, rule: Config_Rule_Data_Model):
        self.deviceid = deviceid
        self.datatype = rule.device_type
        self.start_time = start_time
        self.end_time = end_time
        self.average_value = Decimal(average)
        self.rule_id = rule.id
        self.breach_type=rule.rule_type
        self.rule = str(json.dumps(rule,default=vars))
        self.id = str(uuid.uuid4())
        self.timestamp = str(datetime.datetime.now())
