import boto3


def create_table(table_name='bash_data'):
    """
    Creates a DynamoDB table.

    :param table_name:
    :return: The newly created table.
    """

    #client = boto3.client('dynamodb')
    client = boto3.client('dynamodb', region_name='us-east-1')  # replace 'us-west-1' with your desired region

    response = client.list_tables()

    for existing_table_name in response['TableNames']:
        if existing_table_name == table_name:
            print("skip creating the table : ", table_name)
            return

    dyn_resource = boto3.resource('dynamodb')

    params = {'TableName': table_name, 'KeySchema': [{'AttributeName': 'deviceid', 'KeyType': 'HASH'},
        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}],
        'AttributeDefinitions': [{'AttributeName': 'deviceid', 'AttributeType': 'S'},
            {'AttributeName': 'timestamp', 'AttributeType': 'S'}],
        'ProvisionedThroughput': {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10}}
    table = dyn_resource.create_table(**params)
    print(f"Creating {table_name}...")
    table.wait_until_exists()
    print(f"Created table.")
    return table
