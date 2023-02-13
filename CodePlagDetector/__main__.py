"""
Command-line interface for the CodePlagiarism detector.
Using this, we can run the code plagarism detector using
`python -m CodePlagDetector` command with some arguments.
"""

import argparse
import sys
import os

# This is assuming that the github folder is in the home directory
sys.path.append(os.path.expanduser('~/CodePlagDetector'))
from CodePlagDetector import CodePlagiarismDetector, defaults


def str2bool(v):
  """
  To parse boolean arguments from the command line.
  https://stackoverflow.com/a/43357954
  """
  if isinstance(v, bool):
      return v
  if v.lower() in ('yes', 'true', 't', 'y', '1'):
      return True
  elif v.lower() in ('no', 'false', 'f', 'n', '0'):
      return False
  else:
      raise argparse.ArgumentTypeError('Boolean value expected.')

def threshold(value):
  """
  To parse the threshold arguments from the command line. And to
  verify that the threshold is between 0 and 1.
  """
  try:
    value = float(value)
  except ValueError:
    raise argparse.ArgumentTypeError("Threshold must be a float")
  if value < 0 or value > 1:
    raise argparse.ArgumentTypeError("Threshold must be between 0 and 1")
  return value


def main():
  """
  Main function for parsing command line arguments and running the detector.
  """
  parser = argparse.ArgumentParser(
    prog="CodePlagDetector", formatter_class=argparse.ArgumentDefaultsHelpFormatter
  )
  parser.add_argument("-b", "--bucket-name", required=True,
                      metavar="BUCKET-NAME", help="name of the bucket where the files are stored"
                      " (default: lti-development-bucket)")
  parser.add_argument("-bp", "--boilerplate-prefix", type=str, required=True,
                      metavar="BPREFIX", help="prefix of the bucket where the boilerplate files"
                      " are stored")
  parser.add_argument("-sp", "--submission-prefix", type=str, required=True,
                      metavar="SPREFIX", help="prefix of the bucket where the submissions"
                      " files are stored")
  parser.add_argument("-env", "--environment", type=str, required=True,
                      metavar="ENVIRONMENT", help="Which environment is this running on ? development or migration or production ?")
  parser.add_argument("-e", "--extensions", default=defaults.EXTENSIONS,
                      metavar="EXTENSIONS", help="extensions of the files to be compared")
  parser.add_argument("-n", "--noise-threshold", default=defaults.NOISE_THRESHOLD, type=int,
                      metavar="NOISE-THRESHOLD", help="noise threshold (default: 25)")
  parser.add_argument("-g", "--guarantee-threshold", default=defaults.GUARANTEE_THRESHOLD, type=int,
                      metavar="GUARANTEE-THRESHOLD", help="guarantee threshold (default: 25)")
  parser.add_argument("-d", "--display-threshold", default=defaults.DISPLAY_THRESHOLD, type=threshold,
                      metavar="DISPLAY-THRESHOLD", help="display threshold (default: 0.33)")
  parser.add_argument("-sn", "--same-name-only", default=defaults.SAME_NAME_ONLY,  type=str2bool,
                      metavar="SAME-NAME-ONLY", help="same name only (default: True)")
  parser.add_argument("-fsd", "--fsd", type=str2bool, required=True,
                      metavar="FSD", help="Full stack assignment")
  parser.add_argument("-s", "--silent", default=True, type=str2bool,
                      metavar="SILENT" ,help="To output logs to terminal")

  args = parser.parse_args()

  # handle extensions argument
  if args.extensions:
    args.extensions = [ext.strip() for ext in args.extensions.strip().split(',')]
  
  # defining so that we can check in the finally block
  detector = None
  try:
    detector = CodePlagiarismDetector(args.bucket_name, bprefix=args.boilerplate_prefix,
      sprefix=args.submission_prefix, env=args.environment, noise_t=args.noise_threshold,
      guarantee_t=args.guarantee_threshold, display_t=args.display_threshold,
      same_name_only=args.same_name_only, fsd=args.fsd, extensions=args.extensions
    )
    detector.initialize()
    detector.run()
    detector.upload_reports()

  except Exception as e:
    raise e
  finally:
    if detector:
      detector.clean_up()

if __name__ == '__main__':
  main()