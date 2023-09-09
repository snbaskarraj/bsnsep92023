import boto3
from decimal import Decimal
import datetime
import uuid

# Given Aggregated_Data_Model class
class Aggregated_Data_Model:
    def __init__(self, deviceid, device_type, avg, minm, maxm, start_time, end_time):
        self.device_type = device_type
        self.average = Decimal(avg)
        self.minimum = Decimal(minm)
        self.maximum = Decimal(maxm)
        self.start_time = str(start_time)
        self.end_time = str(end_time)
        self.deviceid = deviceid
        self.id = str(uuid.uuid4())
        self.timestamp = str(datetime.datetime.now())

    def to_dict(self):
        return {
            "id": self.id,
            "deviceid": self.deviceid,
            "device_type": self.device_type,
            "average": str(self.average),
            "minimum": str(self.minimum),
            "maximum": str(self.maximum),
            "start_time": self.start_time,
            "end_time": self.end_time,
            "timestamp": self.timestamp
        }

def insert_data_into_dynamodb(aggregated_data_model):
    # Initialize a DynamoDB resource with boto3
    dynamodb = boto3.resource('dynamodb')

    # Specify the table name
    table = dynamodb.Table('BSM_agg_data')

    # Insert the record
    response = table.put_item(Item=aggregated_data_model.to_dict())
    return response

# Test the insertion
agdm = Aggregated_Data_Model(deviceid="someID", device_type="HeartRate", avg="85.5", minm="75", maxm="90", start_time="2023-09-07 13:05:00", end_time="2023-09-07 13:06:00")
response = insert_data_into_dynamodb(agdm)
print(response)
