from boto3 import client
from boto3 import resource

LOG_FILE = './processed_keys.txt'

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

    # you can use file methods to log processed files
    count = 0
    string_pattern = 'folder_name/2020_'
    conn = client('s3')
    bucket_name = 'valid-bucket-name'

    paginator = conn.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)
     
    for page in pages:
        for object in page['Contents']:
            if object['Key'].startswith(string_pattern):
                #put your code here
                count = count + 1
    print("{} total files".format(count))
