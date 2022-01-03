import boto3


# Local path to download AWS dataset
AWS_DEST_FILE = 'local_path/aws.csv'

class AWSS3Class(object):

    def __init__(self, bucket_name, aws_key):
        self.aws_key = aws_key
        self.bucket_name = bucket_name

    def download(self, name):
        # Init client
        s3 = boto3.resource('s3')
        try:
            # Downloading file
            s3.Bucket(self.bucket_name).download_file(self.aws_key, AWS_DEST_FILE)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise Exception
