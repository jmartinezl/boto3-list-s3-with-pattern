import logging
import json
import boto3
import botocore


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

FOLDER = 'folder-name'
BUCKET = 'bucket-name'


def get_sts_credentials():
    sts_connection = boto3.client('sts')
    sts_credentials = sts_connection.assume_role(
        RoleArn="arn:aws:iam::account-number:role/role-name",
        RoleSessionName="role_session_name"
    )
    return sts_credentials


def _find_s3_record(file_name):

    credentials = get_sts_credentials()

    # create service client using the assumed role credentials, e.g. S3
    resource = boto3.resource(
        's3',
        aws_access_key_id=credentials['Credentials']['AccessKeyId'],
        aws_secret_access_key=credentials['Credentials']['SecretAccessKey'],
        aws_session_token=credentials['Credentials']['SessionToken'],
    )

    try:
        LOGGER.info(FOLDER + file_name)
        resource.Object(BUCKET, FOLDER + file_name).get()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "AccessDenied":
            return True
        elif e.response['Error']['Code'] == "NoSuchKey":
            return False
        else:
            LOGGER.error(str(e))
            return False
    else:
        return True

def _get_by_scan(string_value):

    credentials = get_sts_credentials()

    
    client = boto3.client(
        's3',
        aws_access_key_id=credentials['Credentials']['AccessKeyId'],
        aws_secret_access_key=credentials['Credentials']['SecretAccessKey'],
        aws_session_token=credentials['Credentials']['SessionToken'],
    )

    count = 0

    LOGGER.info(string_value)

    for key in client.list_objects(Bucket=BUCKET)['Contents']:
        file_name = key['Key']

        
        if string_value in file_name:
            LOGGER.info(file_name)
            count = count + 1
    LOGGER.info("{} archivos econtrados".format(count))
    return count


def lambda_handler(event, context):  

    try:
        _value = ''
        if event.get('file_name'):
            _value = event['file_name']
            result = _find_s3_record(_value)

        elif event.get('search_term'):
            _value = event['search_term']
            result = _get_by_scan(_value)
 
        else:
            return create_response("ERROR", "MISSING_PARAMETERS", "Missing parameters.")

        if result:
            json_data = json.dumps(result, ensure_ascii=False,)
            return create_response("OK", "", json_data)
        else:
            return create_response("ERROR", "NO_FILE_FOUND", "File key wasn't found")

    except Exception as e:
        LOGGER.error(str(e))
        return create_response("ERROR", "INTERNAL_ERROR", "Error Interno")


def create_response(status, error, content):
    return {
        "status": status,
        "error": error,
        "content": content
    }
