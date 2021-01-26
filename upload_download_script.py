from boto3 import client
from boto3 import resource
import time

LOG_FILE = './key.log'
MISSING = './data_keys_missing.txt'

def load_file(file):
    """Loads the log file and creates it if it doesn't exist.
     Parameters
    ----------
    file : str
        The file to write down
    Returns
    -------
    list
        A list of urls.
    """

    try:
        with open(file, 'r', encoding='utf-8') as temp_file:
            return temp_file.read().splitlines()
    except Exception:

        with open(LOG_FILE, 'w', encoding='utf-8') as temp_file:
            return []


def update_file(file, data):
    """Updates the log file.
    Parameters
    ----------
    file : str
        The file to write down.
    data : str
        The data to log.
    """

    with open(file, 'a', encoding='utf-8') as temp_file:
        temp_file.write(data + '\n')


if __name__ == '__main__':


    count = 0
    log = load_file(LOG_FILE)
    _missing_files = load_file(MISSING)

    conn = client('s3')
    s3 = resource('s3')
    bucket_name = 'bucket_name'
    startswith_pattern = 'folder/files_start_with_this_string'
    bucket = s3.Bucket(bucket_name)

    paginator = conn.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)
     
    for page in pages:
        for object in page['Contents']:
            file_name = object['Key']
            log = load_file(LOG_FILE)
            if file_name.startswith(startswith_pattern) and file_name.split('/')[1] in _missing_files:
                try:
                    print(file_name)
                    temp_file_name = file_name.split('/')[-1]
                    print(temp_file_name)
                    #code to download the file
                    print("downloading")
                    bucket.download_file(file_name, '/tmp/{}'.format(temp_file_name))
                    #code to upload new file; advice: use versions
                    print("uploading")
                    #you can change /tmp/ for the location of your files
                    bucket.upload_file('/tmp/{}'.format(temp_file_name),file_name)
                    update_file(LOG_FILE, file_name)
                    count = count + 1
                    #just to avoid problems
                    time.sleep(0.5)
                except Exception as e:
                    raise e
    print("{} processed file(s)".format(count))
