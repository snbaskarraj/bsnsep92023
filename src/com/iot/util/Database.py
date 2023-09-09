
import boto3
from boto3.dynamodb.conditions import Key

from src.com.iot.util import DynamoDBCreator


def get_registered_devices():
    return ["BSM_01", "BSM_02", "BSM_03","BSM_G101","BSM_G102"]


# convert object to dynamodb compatible dict
def marshall_data(obj):
    data = {}
    for variable, value in vars(obj).items():
        data[variable] = value
    return data


def create_dynamodb_table(tableName):
    DynamoDBCreator.create_table(tableName)


def get_invalid_rule_value():
    return 99999999999


class Database:

    def __init__(self, table_name):
        self._table_name = table_name
        self._dynamodb = boto3.resource('dynamodb')
        self._table = self._dynamodb.Table(table_name)

    def insert_data(self, data):
        self._table.put_item(Item=data)

    def get_device_raw_data(self, from_date, to_date):
        bsm_data_table = boto3.resource('dynamodb')
        response = bsm_data_table.Table("bsm_data").scan()
        return response["Items"]

    def get_data_from_table(self, table_name, deviceid=""):
        bsm_data_table = boto3.resource('dynamodb')
        response = []
        if len(deviceid) > 0:
            response = bsm_data_table.Table(table_name).query(
                KeyConditionExpression=Key('deviceid').eq(deviceid)
            )
            #
            # dynamodb_client = boto3.client('dynamodb')
            # response = dynamodb_client.query(TableName=table_name, KeyConditionExpression='deviceid = :deviceid',
            #     ExpressionAttributeValues={':deviceid': {'S': deviceid}})
        else:
            response = bsm_data_table.Table(table_name).scan()

        return response["Items"]

