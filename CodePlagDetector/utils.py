import boto3
from pathlib import Path
import zipfile

from .exceptions import s3ConnectionError, NoBucketProvidedError
import logging
import numpy as np
import json
import os


def get_s3_bucket(bucket_name, region_name='us-east-1'):
  """
  This function returns the S3 bucket object if the bucket exists and the
  credentials are valid. Otherwise, it returns False.

  Parameters
  ----------
    secret_key  : the secret key of the AWS account
    access_key  : the access key of the AWS account
    bucket_name : the name of the S3 bucket
    region_name : the region name of the S3 bucket. Default is us-east-1

  Returns
  -------
    The S3 bucket object if the bucket exists and the credentials are valid. Otherwise,
    throws an error.
  """
  print("Connecting to S3...", end=' ')
  if bucket_name is None:
    raise NoBucketProvidedError("No bucket provided.")
  
  access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
  access_key_secret = os.environ.get('AWS_SECRET_ACCESS_KEY')
  if access_key_id is None or access_key_secret is None:
    raise s3ConnectionError("AWS credentials not provided.")
  
  s3 = boto3.resource(
    service_name='s3',
    region_name=region_name,
    aws_access_key_id=access_key_id,
    aws_secret_access_key=access_key_secret
  )
  try:
    s3.meta.client.head_bucket(Bucket=bucket_name)
    print("successful!\n")
    return s3.Bucket(bucket_name)
  except:
    errorMsg = "Failed to connect to s3 bucket: {}".format(bucket_name)
    logging.error(errorMsg)
    raise s3ConnectionError(errorMsg)


# download the files
def download_files_with_prefix(bucket, prefix, rootDir='', silent=True, fsd=True):
  """
  This function downloads all the files from the bucket with the given prefix
  to the destDir directory inside the home directory(home/CodePlagDetector/{destDir}/{obj.key}).

  As prefix would generally be the AssignmentID or SubmissionID, the files will be downloaded to
  home/CodePlagDetector/{AssignmentID or SubmissionID} folder.

  Note: The obj.key would already have the prefix in it. So, we don't need to
        add the prefix directory while downloading the files.

  PARAMETERS
  ----------
    bucket  : the S3 bucket object
    prefix  : the prefix of the files in the bucket. Used to download specific files in the bucket
    rootDir : the root directory where the files will be downloaded to inside the home directory.
              If not give, the files will be downloaded to the home directory with the 
              folder structure as per the obj.key

  RETURNS
  -------
    None

  """
  # download the files from the bucket with prefix CodePlagiarism
  for obj in bucket.objects.filter(Prefix=prefix):
    destFilePath = Path(os.path.expanduser('~')).joinpath(rootDir, obj.key)
    # create all the necessary parent directories if not present
    if not Path(destFilePath).parent.exists():
      Path(destFilePath).parent.mkdir(parents=True)
  
    # if it is a file then download
    if obj.key[-1] != '/':
      if not Path.exists(destFilePath):
        if fsd:
          if obj.key[-4:] == '.zip':
            if not silent:
              print('Downloading', obj.key, 'to', destFilePath)
            # bucket.download_file(obj.key, destFilePath, destFilePath)
            # The above thing is not working. So, using the client to download the file
            bucket.meta.client.download_file(bucket.name, obj.key, destFilePath.as_posix())
            # extracting abc/xyz.zip to abc/xyz folder
            if Path(destFilePath).parent.joinpath(Path(destFilePath).name[:-4]).exists():
              Path(destFilePath).parent.joinpath(Path(destFilePath).name[:-4]).mkdir(parents=True)
            with zipfile.ZipFile(destFilePath.as_posix(), 'r') as zip_ref:
              zip_ref.extractall(Path(destFilePath).parent.joinpath(Path(destFilePath).name[:-4]).as_posix())
        else:
          if not silent:
            print('Downloading', obj.key, 'to', destFilePath)
          bucket.meta.client.download_file(bucket.name, obj.key, destFilePath.as_posix())
      else:
        if not silent:
          print('Already downloaded', obj.key)


# https://stackoverflow.com/a/57915246
# encoder to convert numpy types to python types befpre writing to the json file
class NumpyEncoder(json.JSONEncoder):
  """
  Special json encoder for numpy types
  
  RETURNS
  -------
    JSONEncoder object with necessary methods to encode numpy types
  """
  def default(self, obj):
    if isinstance(obj, np.integer):
      return int(obj)
    elif isinstance(obj, np.floating):
      return float(obj)
    elif isinstance(obj, np.ndarray):
      return obj.tolist()
    else:
      return super(NumpyEncoder, self).default(obj)
