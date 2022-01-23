import boto3

# Local path to download AWS dataset
AWS_DEST_FILE = 'C:/Users/jagta/Desktop/downloaded_store.csv'

class AWSS3Class(object):

    def __init__(self, bucket_name, aws_key_id, aws_access_key):
        self.aws_key_id = aws_key_id
        self.aws_access_key = aws_access_key
        self.bucket_name = bucket_name

    def download(self, name: str, download_path: str):
        # Init client
        s3 = boto3.resource('s3', aws_access_key_id=self.aws_key_id, aws_secret_access_key=self.aws_access_key)
        try:
            # Downloading file
            s3.Bucket(self.bucket_name).download_file(Key=name, Filename=download_path)
        except Exception as e:
            print("error encountered when trying to connect to AWS: {}".format(e))
