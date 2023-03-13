"""
Command-line interface for the CodePlagiarism detector.
Using this, we can run the code plagarism detector using
`python -m CodePlagDetector` command with some arguments.
"""

import argparse
import sys
import os
import traceback

# This is assuming that the github folder is in the home directory
sys.path.append(os.path.expanduser('~/CodePlagDetector'))
from CodePlagDetector import CodePlagiarismDetector, defaults, utils


def main():
  """
  Main function for parsing command line arguments and running the detector.
  """
  parser = argparse.ArgumentParser(
    prog="CodePlagDetector", formatter_class=argparse.ArgumentDefaultsHelpFormatter
  )
  parser.add_argument("-details-api", "--details-url", type=str, required=True)
  parser.add_argument("-update-url", "--update-url", type=str, required=True)

  args = parser.parse_args()
  try:
    # make request and get the arguments
    params = utils.make_request(args.details_url)
  except Exception as e:
    print('Error while making request to', args.details_url, 'with error', e)
    utils.make_request(args.update_url, 'UPDATE', data={
      'scan_id': args.details_url.split('?')[1].split('&')[0].split('=')[1], 'status': 'FAILED', 'error': traceback.format_exc()
    })
    return

  try:
    # form the other arguments
    noise_threshold = params.get('noise_threshold', defaults.NOISE_THRESHOLD)
    guarantee_threshold = params.get('guarantee_threshold', defaults.GUARANTEE_THRESHOLD)
    display_threshold = params.get('display_threshold', defaults.DISPLAY_THRESHOLD)
    fsd = params.get('submission_type') == 'fsd'
    extensions = params.get('extensions', '*').split(',')
    silent = params.get('silent', True)

    # defining so that we can check in the finally block
    detector = None
    detector = CodePlagiarismDetector(params['scan_id'], params['bucket_name'], sprefix=params['submission_prefix'],
      bprefix=params['boilerplate_prefix'], env=params['environment'], users_to_scan=params['users_to_scan'],
      extensions=extensions, noise_t=noise_threshold,  guarantee_t=guarantee_threshold, display_t=display_threshold,
      silent=silent, same_name_only=params['same_name_only'], fsd=fsd, update_frequency=params['update_frequency'],
      update_url=args.update_url
    )
    detector.initialize()
    
    utils.make_request(detector.update_url, 'UPDATE', data={'scan_id': detector.scan_id, 'status': 'STARTED'})
    detector.run()
    utils.make_request(detector.update_url, 'UPDATE', data={'scan_id': detector.scan_id, 'status': 'COMPLETED'})
  except Exception as e:
    # if it failed, then update the status
    utils.make_request(args.update_url, 'UPDATE', data={
      'scan_id': params['scan_id'], 'status': 'FAILED', 'error': traceback.format_exc()
    })
  finally:
    if detector:
      detector.clean_up()

if __name__ == '__main__':
  main()