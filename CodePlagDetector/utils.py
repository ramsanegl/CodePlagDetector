import boto3
from pathlib import Path
import zipfile

from .exceptions import s3ConnectionError, NoBucketProvidedError
import logging
import numpy as np
import json
import os

import time
import random
import string


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
    errorMsg = f"Failed to connect to s3 bucket: {bucket_name}"
    logging.error(errorMsg)
    raise s3ConnectionError(errorMsg)


def download_files_for_codeeval(bucket, prefix, rootDir='', silent=True):
  """
  This is to download files for codeeval. Here, we assume that the boilerplate
  and the submission paths are different.
  """
  for obj in bucket.objects.filter(Prefix=prefix):
    destFilePath = Path.home().joinpath(rootDir, prefix, obj.key.replace(prefix, ''))
    download_from_s3(bucket, obj.key, destFilePath, silent=silent)

def download_from_s3(bucket, object_key, destFilePath, silent=True):
  """
  This function downloads a file from the S3 bucket to the destFilePath.

  Parameters
  ----------
    bucket       : The S3 bucket object
    object_key   : The key of the object in the bucket
    destFilePath : The path where the file will be downloaded to. This path should include
                   the file name as well.
  Returns
  -------
    None
  """
  # create all the necessary parent directories if not present
  Path(destFilePath).parent.mkdir(parents=True, exist_ok=True)
  if not Path.exists(destFilePath):
    if not silent:
      print('Downloading', object_key, ' to ', destFilePath)
    bucket.download_file(object_key, destFilePath)
  else:
    if not silent:
      print('Already downloaded', object_key)

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
    destFilePath = Path.home().joinpath(rootDir, obj.key)
    # create all the necessary parent directories if not present
    Path(destFilePath).parent.mkdir(parents=True, exist_ok=True)
    # if it is a file then download
    if obj.key[-1] != '/':
      if not Path.exists(destFilePath):
        # if fsd, then download only zip files
        if fsd:
          if obj.key[-4:] == '.zip':
            if not silent:
              print('Downloading', obj.key, 'to', destFilePath)
            bucket.download_file(obj.key, destFilePath)
            # extracting abc/xyz.zip to abc/xyz folder
            Path(destFilePath).parent.joinpath(Path(destFilePath).name[:-4]).mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(destFilePath, 'r') as zip_ref:
              zip_ref.extractall(Path(destFilePath).parent.joinpath(Path(destFilePath).name[:-4]))
        else:
          if not silent:
            print('Downloading', obj.key, 'to', destFilePath)
          bucket.download_file(obj.key, destFilePath)
      else:
        if not silent:
          print('Already downloaded', obj.key)


def get_random_string(length):
  """
  This function generates a random string of given length
  with timestamp.
  PARAMETERS
  ----------
    length : the length of the random string
  RETURNS
  -------
    timestamp_randomstring : the random string with timestamp
  """
  timestamp = time.strftime("%Y%m%d%H%M%S")
  random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
  return timestamp + '_' + random_string


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