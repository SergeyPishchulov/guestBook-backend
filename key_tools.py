import requests


def get_iam_token():
    response = requests.get('http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token',
                            headers={"Metadata-Flavor": "Google"})
    iam_token = response.json()['access_token']
    return iam_token

def get_keys():
    iam_token = get_iam_token()

    lockbox_secret_id = 'e6q70pjpnij0o4mntpje'
    aws_secrets_response = requests.get(
        f'https://payload.lockbox.api.cloud.yandex.net/lockbox/v1/secrets/{lockbox_secret_id}/payload',
        headers={"Authorization": f"Bearer {iam_token}"})
    j = aws_secrets_response.json()
    aws_access_key_id, aws_secret_access_key = None, None
    for key_value_dict in j['entries']:
        if key_value_dict['key'] == 'aws_access_key_id':
            aws_access_key_id = key_value_dict['textValue']
        if key_value_dict['key'] == 'aws_secret_access_key':
            aws_secret_access_key = key_value_dict['textValue']

    if not aws_access_key_id or not aws_secret_access_key:
        raise Exception("Couldn't get aws secrets")
    return aws_access_key_id, aws_secret_access_key
