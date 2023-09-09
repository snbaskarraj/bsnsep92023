import json
import boto3
from boto3.dynamodb.conditions import Attr

# Database helper class
class Database:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.raw_data_table = self.dynamodb.Table('BSM_DATA')
        self.aggregated_data_table = self.dynamodb.Table('BSM_agg_data')

    def get_raw_data(self, start_time, end_time):
        response = self.raw_data_table.scan(
            FilterExpression=Attr('timestamp').between(start_time, end_time)
        )
        print(f"DynamoDB response: {response}")
        return response.get('Items', [])





    def store_aggregated_data(self, aggregated_data):
        for agg_data in aggregated_data:
            self.aggregated_data_table.put_item(Item=agg_data)

# Load the rules from JSON file
def load_rules_from_file():
    with open("../../../anomaly_rules/config_rule.json", "r") as file:
        return json.load(file)

# Aggregate data based on the rules
def aggregate_data(start_time, end_time):
    db = Database()
    raw_data = db.get_raw_data(start_time, end_time)
    print(f"Fetched raw data: {raw_data}")

    rules = load_rules_from_file()
    print(f"Loaded rules: {rules}")

    aggregated_data = []

    for rule in rules:
        matching_data_points = [
            data for data in raw_data if 'device_type' in data and data['device_type'] == rule['device_type']
        ]

        print(f"Matching data points for {rule['device_type']}: {matching_data_points}")

        if not matching_data_points:
            print(f"No data points found for {rule['device_type']}. Skipping aggregation.")
            continue

        avg_value = sum([data['value'] for data in matching_data_points]) / len(matching_data_points)
        print(f"Calculated average value for {rule['device_type']}: {avg_value}")

        if rule['rule_type'] == 'min' and avg_value < rule['avg_min']:
            aggregated_data.append({
                'type': rule['device_type'],
                'avg_value': avg_value,
                'start_time': start_time,
                'end_time': end_time,
            })
        elif rule['rule_type'] == 'max' and avg_value > rule['avg_max']:
            aggregated_data.append({
                'type': rule['device_type'],
                'avg_value': avg_value,
                'start_time': start_time,
                'end_time': end_time,
            })
        elif rule['rule_type'] == 'min_max' and (avg_value < rule['avg_min'] or avg_value > rule['avg_max']):
            aggregated_data.append({
                'type': rule['device_type'],
                'avg_value': avg_value,
                'start_time': start_time,
                'end_time': end_time,
            })

    db.store_aggregated_data(aggregated_data)

# Main execution
if __name__ == "__main__":
    aggregate_data("2023-09-07 13:05:38", "2023-09-07 13:08:00")

