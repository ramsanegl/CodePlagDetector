
import logging
import time
from collections import defaultdict
from tqdm import tqdm
from pathlib import Path
import json
import os
import re
import numpy as np

from copydetect import CopyDetector, compare_files
# imports from CodePlagDetector.py
from .utils import (download_files_for_codeeval,download_files_with_prefix,
  get_random_string, get_s3_bucket, NumpyEncoder, make_request
)
from .exceptions import NoFilesFoundError



class CodePlagiarismDetector:
  """
  This class is responsible for detecting plagiarism in the given code files.
  It uses the CopyDetector class from copydetect module to detect plagiarism.

  PARAMETERS
  ----------
    scanId         :  The scanID in the database
    bucket_name    :  The name of the S3 bucket
    sprefix        :  The prefix of where the submission files are stored in the bucket.
    bprefix        :  The prefix of where the boilerplate files are stored in the bucket.
    env            :  The environment in which the code is running. It can be one of
                      ['development', 'migration', 'production']
    users_to_scan  :  The list of users whose files will be scanned. If None, then all the users
    rootDir        :  The root directory where the files will be downloaded to inside the home directory.
                      Default is CodePlagDetector
    extensions     :  The extensions of the files to be compared. Default is ['.java']
    noise_t        :  The smallest sequence of matching characters between two files which
                      **should** be considered plagiarism.
    guarantee_t    :  The smallest sequence of matching characters between two files for which the system is
                      guaranteed to detect a matchDefault is 25.
                      This must be greater than or equal to noise_t
    same_name_only :  If True, then it will only compare the files with the same name. We should
                      avoid this if the files are not named properly. Default is True.
    display_t      :  The similarity threshold to flag plagiarism. Default is 0.33
    silent         :  If True, then it will not print any logs. Default is True.
    fsd            :  If True, then we will download only zip files. Default is False
    update_frequency : The frequency at which the progress of the scan will be updated in the database.
                        Default is 5
    update_url     : The url to which the progress of the scan will be updated. Default is None
  """


  def __init__(self, scanID: int, bucket_name: str, sprefix: str, bprefix:str, env:str, users_to_scan: list or None,
                  rootDir='CodePlagiarism/', extensions = ['*'], noise_t = 25, guarantee_t = 25,same_name_only=True,
                  display_t=0.33, silent=True, fsd=False, update_frequency=5, update_url=None):
    """
    Connect to S3 bucket and initialize the detector object with the given params
    """
    self.scanID = scanID
    self.bucket_name = bucket_name
    self.sprefix = sprefix
    self.bprefix = bprefix
    self.env = env
    self.users_to_scan = users_to_scan
    # for rootDir, we are adding an additional folder so that we can maintain
    # a separate folder for each scan
    self.rootDir = rootDir + str(scanID) + "/"

    self.noise_t = noise_t
    self.guarantee_t = guarantee_t
    self.same_name_only = same_name_only
    self.silent = silent
    self.display_t = display_t
    self.extensions = extensions
    self.fsd = fsd
    self.update_frequency = update_frequency
    self.update_url = update_url
    self.detector = None
    self.reportDir = "Reports"

    self._validate_fields()

    # connect to the S3 bucket
    self.bucket = get_s3_bucket(self.bucket_name, self.env)
  

  def _validate_fields(self):
    """
    validates different input params
    """
    if self.scanID is None:
      raise ValueError("Scan ID cannot be None")
    if self.bucket_name is None:
      raise ValueError("Bucket name cannot be None")
    if self.sprefix is None:
      raise ValueError("Submissions prefix cannot be None")
    if self.bprefix is None:
      raise ValueError("Boilerplate prefix cannot be None")
    if self.env is None or self.env not in ['development', 'migration', 'production']:
      raise ValueError("Environment cannot be None and must be one of 'development', 'migration' or 'production'")
    if self.users_to_scan is not None and not isinstance(self.users_to_scan, list):
      raise ValueError("users_to_scan must be a list of users")
    if self.update_url is None:
      raise ValueError("update_url is required")
    
    # adding the trailing slash if not present
    if self.sprefix[-1] != '/':
      self.sprefix += '/'
    if self.bprefix[-1] != '/':
      self.bprefix += '/'

  
  def download_files(self):
    """
    Download the files from the bucket with the given prefix.
    It will download them to the home directory inside CodePlagDetector folder.
    And if there is any zip file, then it's unzip them to the same folder to a
    folder with the same name as the zip file.
    """
    if len(list(self.bucket.objects.filter(Prefix=self.sprefix).limit(1))) == 0:
      errorMsg = "No files found in the bucket with prefix: {}".format(self.sprefix)
      logging.error(errorMsg)
      raise NoFilesFoundError(errorMsg)
  
    if len(list(self.bucket.objects.filter(Prefix=self.bprefix).limit(1))) == 0:
      errorMsg = "No files found in the bucket with prefix: {}".format(self.bprefix)
      logging.error(errorMsg)
      raise NoFilesFoundError(errorMsg)

    if self.fsd:
      raise NotImplementedError("fsd is not implemented yet")
      # download and unzip the files, if there are any .zip files (only .zip is supported)
      # download_files_with_prefix(self.bucket, prefix=self.prefix, rootDir=self.rootDir,
      #                           silent=self.silent, fsd=self.fsd)
    else:
      download_files_for_codeeval(self.bucket, prefix=self.bprefix, extensions=self.extensions, rootDir=self.rootDir, silent=self.silent, boilerplate=True)
      download_files_for_codeeval(self.bucket, prefix=self.sprefix, extensions=self.extensions, rootDir=self.rootDir, silent=self.silent)

  
  def initialize(self):
    """
    It will download the files from the bucket and initialize the detector object
    """
    # download the files from the bucket
    self.download_files()
    make_request(self.update_url, 'UPDATE', data={'scanID': self.scanID, 'status': 'DOWNLOADED'})

    # create the detector object with appropriate params 
    self.detector = CopyDetector(
      boilerplate_dirs=[Path(os.path.expanduser('~')).joinpath(self.rootDir, self.bprefix).as_posix()],
      test_dirs=[Path(os.path.expanduser('~')).joinpath(self.rootDir, self.sprefix).as_posix()],
      noise_t=self.noise_t,
      guarantee_t=self.guarantee_t,
      display_t=self.display_t,
      ignore_leaf=True,
      same_name_only=self.same_name_only,
      extensions=self.extensions,
    )
    # checking if we have any files in the test and ref directories to compare
    if len(self.detector.test_files) == 0:
      logging.error("Code plagiarism failed: No files found in "
                    "test directories")
      raise NoFilesFoundError("No files found in Test directories.")
    if len(self.detector.ref_files) == 0:
      logging.error("Code plagiarism failed: No files found in "
                    "reference directories")
      raise NoFilesFoundError("No files found in Reference directories.")


  def run(self):
    start_time = time.time()
    print("  0.00: Generating file fingerprints")
    # generate fingerprints for all files after winnowing.
    self.detector._preprocess_code(self.detector.test_files + self.detector.ref_files)
    
    # split the test files for each student
    # this is to faciliate the scan for every student individually
    test_files_student_dict = defaultdict(list)
    for test_file in self.detector.test_files:
        student_id = re.search(r'users/(\d+)/', test_file).group(1)
        test_files_student_dict[student_id].append(test_file)
    
    # filter the users to scan if specified.
    final_test_files_student_dict = defaultdict(list)
    # if users list is None, we will consider it as full scan.
    if self.users_to_scan is None or (isinstance(self.users_to_scan, list) and len(self.users_to_scan)==0):
      final_test_files_student_dict = test_files_student_dict
    # else include only the students who are in the include list
    for student, test_files in test_files_student_dict.items():
        if isinstance(self.users_to_scan, list) and int(student) in self.users_to_scan:
            final_test_files_student_dict[student] = test_files
    
    # create the report directory inside the prefix folder in the root directory.
    if not Path(os.path.expanduser('~')).joinpath(self.rootDir, self.sprefix, self.reportDir, str(self.scanID)).exists():
      Path(os.path.expanduser('~')).joinpath(self.rootDir, self.sprefix, self.reportDir, str(self.scanID)).mkdir(parents=True)

    user_reports = {}  # used to store the user_ids that we wish to update in between the scan.
    print("{:6.2f}: Beginning code comparison".format(time.time()-start_time))
    for student, test_files in tqdm(final_test_files_student_dict.items(), bar_format='   {l_bar}{bar}{r_bar}'):
      result_dict = {} # used to store the results for this student
      studentReportPath = Path(os.path.expanduser('~')).joinpath(self.rootDir, self.sprefix, self.reportDir, str(self.scanID), "{}.json".format(student))
      # if the report has already been generated, for the student, then skip
      if studentReportPath.exists(): continue
      for test_f in test_files:
        relative_test_f_path = test_f.split(self.detector.test_dirs[0])[1][1:]
        results = []  # used to store the scan results for this test file.
        copied_hashes = np.array([], dtype=np.int64)  # dtype is as per the hashes.
        for ref_f in self.detector.ref_files:
          # if it is out of file_data then continue
          if (
            test_f not in self.detector.file_data or ref_f not in self.detector.file_data
            or test_f == ref_f # if it is the same file
            # if the same name only is true and the names are not the same then continue
            or (self.detector.same_name_only and Path(test_f).name != Path(ref_f).name)
            # if the ignore leaf is true and the parent directories are the same then continue
            or (self.detector.ignore_leaf and Path(test_f).parent == Path(ref_f).parent)
            # if the file extensions are not the same then continue
            or (Path(test_f).suffix != Path(ref_f).suffix)
          ):
            continue

          # get the results
          hashes_overlap1, token_overlap, (sim1, sim2), (slices1, slices2) = compare_files(
              self.detector.file_data[test_f], self.detector.file_data[ref_f],
          )
          # if the similarity is greater than the threshold then append
          if sim1 > self.detector.display_t or sim2 > self.detector.display_t:
            relative_ref_f_path = ref_f.split(self.detector.ref_dirs[0])[1][1:]
            results.append({
              'ref_file': relative_ref_f_path,
              'overlap': token_overlap,
              'test_similarity': sim1,
              'ref_similarity': sim2,
              'test_file_slices': slices1,
              'ref_file_slices': slices2
            })
            # update the copied hashes and the user_ids
            copied_hashes = np.union1d(copied_hashes, hashes_overlap1) 
        # find the overall score for this test file
        if len(copied_hashes) > 0 and len(results) > 0:
          # get the score for this test file
          score = len(copied_hashes)/len(self.detector.file_data[test_f].hashes)
          result_dict[relative_test_f_path] = {
            'score': score,
            'results': results
          }
      
      # Once all the test files are scanned, then save the report to the disk
      if result_dict:
        with open(studentReportPath.as_posix(), 'w') as f:
          json.dump(result_dict, f, indent=2, cls=NumpyEncoder)
        # Update the user_reports with the path to the report.
        # TODO: user result_dict to compute score for each test file and add
        # that in the user_reports. Make the changes in the update api
        # accordingly.
        user_reports[student] = studentReportPath.as_posix()
      else:
        user_reports[student] = ''
      # depending on the update_frequency, upload the reports and update the lti
      if len(user_reports) == self.update_frequency:
        self.upload_reports(user_reports, update=True)
        user_reports = {}

    # upload the remaining reports ( if any )
    if len(user_reports) > 0:
      print('Uploading remaining reports')
      self.upload_reports(user_reports, update=True)
    print("{:6.2f}: Code comparison completed".format(time.time()-start_time))


  # upload the Reports folder to the bucket
  def upload_reports(self, user_reports: dict, update=False):
    """
    Used to upload the reports to the bucket.
    Parameters:
      user_reports (dict): The user reports to be updated. This has user and
                           local report paths as key value pairs.
    """
    # upload the files to the bucket
    final_reports = {}
    if not self.silent:
      print("Uploading {} reports to the bucket".format(len(user_reports)))
    for student, report_file in user_reports.items():
      # prefix already has the forward slash
      s3_key = "{}{}/{}/{}".format(self.sprefix, self.reportDir, str(self.scanID), Path(report_file).name)
      if not self.silent: print("Uploading {} to {}".format(s3_key, self.bucket_name))
      self.bucket.meta.client.upload_file(report_file, self.bucket.name, s3_key)
      # update the user_report
      final_reports[student] = s3_key
    # update the server if specified
    if update:
      data = {
        'scanID': self.scanID,
        'reports': final_reports
      }
      make_request(self.update_url, 'UPDATE', data=data)

  def clean_up(self):
    """
    Used to cleanup the resources like deleting files etc.. But for now we are just
    closing the active s3 connection.
    """
    
    # closing the s3 connection 
    if self.detector is not None:
      # print('\nClosing the s3 connection...', end=' ')
      # closing the s3 connection
      # self.bucket.meta.client._endpoint.http_session.close()
      # print('Done', end='\n\n')
      # we can use this to delete some operations if needed.
      # keeping it empty for now.
      pass
      

