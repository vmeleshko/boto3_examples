import json
from datetime import datetime

import boto3

from logger import logger

TABLE_NAME = 'users-vitalii-test'
BUCKET_NAME = 's3-vitalii-test'
FILE_IN_BUCKET = 'users.txt'


# list all Dynamo DB Tables
def get_all_dynamo_db_table_names(limit=3):
    table_names = []
    client = boto3.client('dynamodb')
    response = client.list_tables(Limit=limit)
    table_names.extend(response['TableNames'])
    while response.get('LastEvaluatedTableName'):
        response = client.list_tables(ExclusiveStartTableName=response['LastEvaluatedTableName'], Limit=limit)
        table_names.extend(response['TableNames'])
    return table_names


# Create the DynamoDB table.
def create_dynamo_db_users_table():
    # check if users table already created
    if TABLE_NAME not in get_all_dynamo_db_table_names():
        try:
            table = boto3.resource('dynamodb').create_table(
                TableName=TABLE_NAME,
                KeySchema=[
                    {
                        'AttributeName': 'username',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'created_at',
                        'KeyType': 'RANGE'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'username',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'created_at',
                        'AttributeType': 'S'
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )

            # Wait until the table exists.
            table.meta.client.get_waiter('table_exists').wait(TableName=TABLE_NAME)

            logger.info(f'DynamoDB Table {TABLE_NAME} successfully created')
        except Exception as e:
            logger.error(f'Failed to create DynamoDB Table {TABLE_NAME} with error: {e.__str__()}')
    else:
        logger.warning(f'DynamoDB Table {TABLE_NAME} already exists')


def populate_table(event=None, context=None):
    create_dynamo_db_users_table()

    s3 = boto3.resource('s3')
    table = boto3.resource('dynamodb').Table(TABLE_NAME)

    # obj = list(filter(lambda x: x.key == FILE_IN_BUCKET, s3.Bucket(BUCKET_NAME).objects.all()))[0]
    obj = s3.Object(BUCKET_NAME, FILE_IN_BUCKET)

    body = obj.get()['Body'].read()
    print(body)
    body = body.decode()

    for line in body.split():
        username, color = line.split(',')
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item = {
            'username': username,
            'color': color,
            'created_at': created_at,
        }
        table.put_item(Item=item)
        logger.info(f'Item {item} saved into table {TABLE_NAME}')

    result = {
        "message": "Go Serverless! Your function executed successfully!",
        "table_name": TABLE_NAME,
        "table_item_count": table.item_count
    }
    response = {
        "statusCode": 200,
        "body": json.dumps(result)
    }

    return response


def delete_table():
    table = boto3.resource('dynamodb').Table(TABLE_NAME)
    table.delete()

    # Wait until the table disappear.
    table.meta.client.get_waiter('table_not_exists').wait(TableName=TABLE_NAME)

    logger.info(f'DynamoDB Table {TABLE_NAME} successfully deleted')


if __name__ == '__main__':
    print(populate_table())
    # delete_table()

    print(TABLE_NAME in get_all_dynamo_db_table_names(limit=100))


    # serverless create -n my-serverless-project -t aws-python3