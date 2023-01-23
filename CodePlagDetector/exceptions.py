"""
This file mainly has exceptions that can be used to do custom
error handling if needed in the code later.
"""


class NoBucketProvidedError(Exception):
  pass

class s3ConnectionError(Exception):
  pass

class NoFilesFoundError(Exception):
  pass