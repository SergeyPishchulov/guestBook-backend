import boto3
import datetime
from key_tools import get_keys

# from local_keys import local_aws_keys

# пример - https://practicum.yandex.ru/trainer/ycloud/lesson/0914346c-22d5-4180-b1c2-2d1134133de7/
# TODO delete
table_name = 'docapi/messagest'


class MessageRepo:
    def __init__(self):
        aws_access_key_id, aws_secret_access_key = get_keys()
        # aws_access_key_id, aws_secret_access_key = local_aws_keys
        self.ydb_docapi_client = boto3.resource('dynamodb'
                                                , region_name='ru-central1-a',
                                                endpoint_url="https://docapi.serverless.yandexcloud.net/ru-central1/b1g6umv05aptrlq8j8il/etn33jkro2i6oq97t0n4",
                                                aws_access_key_id=aws_access_key_id,
                                                aws_secret_access_key=aws_secret_access_key)

    def create_series_table(self):
        table = self.ydb_docapi_client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'message_id',
                    'KeyType': 'HASH'  # Ключ партицирования
                },
                {
                    'AttributeName': 'message',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'message_id',
                    'AttributeType': 'N'  # Целое число
                },
                {
                    'AttributeName': 'message',
                    'AttributeType': 'S'  # Строка
                },
                {
                    'AttributeName': 'timestamp',
                    'AttributeType': 'S'  # Строка
                },

            ]
        )
        return table

    def upload(self, message):
        table = self.ydb_docapi_client.Table(table_name)
        ts = datetime.datetime.now().isoformat()
        table.put_item(
            Item={"message_id": hash(message + ts), 'message': message, 'timestamp': ts})

    def delete_all(self):
        table = self.ydb_docapi_client.Table(table_name)
        scan = table.scan()
        with table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(
                    Key={
                        'message_id': each['message_id'],
                        'message': each['message']
                    }
                )

    # def download(self):
    #     try:
    #         response = table.get_item(Key={'message_id': 1, 'message': "Hi2!"})
    #     except ClientError as e:
    #         print(e.response['Error']['Message'])
    #     else:
    #         return response['Item']

    def get_all(self):
        table = self.ydb_docapi_client.Table(table_name)
        response = table.scan()
        return response['Items']


if __name__ == '__main__':
    # print("Table status:", series_table.table_status)
    repo = MessageRepo()
    # series_table = repo.create_series_table()
    # repo.delete_all()
    print(repo.get_all())

