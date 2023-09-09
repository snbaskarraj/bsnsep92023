import boto3
from decimal import Decimal
import uuid
import datetime

# Your Aggregated Data Model Definition
class Aggregated_Data_Model:
    def __init__(self, deviceid, device_type, avg, minm, maxm, start_time, end_time):
        self.device_type = device_type
        self.average = Decimal(avg)
        self.minimum = Decimal(minm)
        self.maximum = Decimal(maxm)
        self.start_time = str(start_time)
        self.end_time = str(end_time)
        self.deviceid = "BSM_G101"
        self.id = str(uuid.uuid4())
        self.timestamp = str(datetime.datetime.now())

# Initialize a session using Amazon DynamoDB
session = boto3.session.Session(region_name='us-east-1')
dynamodb = session.resource('dynamodb')
table = dynamodb.Table('bsm_agg_data')

# Create an anomalous HeartRate record
anomalous_record = Aggregated_Data_Model(
    deviceid='exampleDeviceId',
    device_type='HeartRate',
    avg=65,
    minm=60,  # You can modify these as needed
    maxm=67,
    start_time='2023-08-14T12:00:00Z',
    end_time='2023-08-14T12:01:00Z'
)

# Insert the anomalous record into the table
response = table.put_item(Item=anomalous_record.__dict__)

print(response)
