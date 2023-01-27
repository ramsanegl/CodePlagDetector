
import logging
import time
from collections import defaultdict
from tqdm import tqdm
from pathlib import Path
import json

from copydetect import CopyDetector, compare_files
# imports from CodePlagDetector.py
from .utils import (download_files_with_prefix,
  get_s3_bucket, NumpyEncoder
)
from .exceptions import NoFilesFoundError



class CodePlagiarismDetector:
  """
  This class is responsible for detecting plagiarism in the given code files.
  It uses the CopyDetector class from copydetect module to detect plagiarism.

  PARAMETERS
  ----------
    bucket_name    :  The name of the S3 bucket
    prefix         :  The prefix of the files in the bucket(eg: AssignmentID). Otherwise,
                      it will assume that the root folder is the prefix
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
    fsd            :  If True, then we will download only zip files.
  """


  def __init__(self, bucket_name: str, prefix: str, rootDir='CodePlagDetector',
                  extensions = ['.java'], noise_t = 25, guarantee_t = 25,
                  same_name_only=True, display_t=0.33, silent=True, fsd=True):
    """
    Connect to S3 bucket and initialize the detector object with the given params
    """
    self.bucket = get_s3_bucket(bucket_name)
    self.prefix = prefix
    self.rootDir = rootDir
    self.noise_t = noise_t
    self.guarantee_t = guarantee_t
    self.same_name_only = same_name_only
    self.silent = silent
    self.display_t = display_t
    self.extensions = extensions
    self.fsd = fsd
    self.detector = None
  

  def download_files(self):
    """
    Download the files from the bucket with the given prefix.
    It will download them to the home directory inside CodePlagDetector folder.
    And if there is any zip file, then it's unzip them to the same folder to a
    folder with the same name as the zip file.
    """
    if len(list(self.bucket.objects.filter(Prefix=self.prefix).limit(1))) == 0:
      errorMsg = f"No files found in the bucket with prefix: {self.prefix}"
      logging.error(errorMsg)
      raise NoFilesFoundError(errorMsg)
    # download and unzip the files, if there are any .zip files (only .zip is supported)
    download_files_with_prefix(self.bucket, prefix=self.prefix, rootDir=self.rootDir,
                              silent=self.silent, fsd=self.fsd)
  

  def initialize(self):
    """
    It will download the files from the bucket and initialize the detector object
    """
    # download the files from the bucket
    self.download_files()

    # create the detector object with appropriate params 
    self.detector = CopyDetector(
      boilerplate_dirs=[Path.home().joinpath(self.rootDir, self.prefix, 'boilerplate').as_posix()],
      test_dirs=[Path.home().joinpath(self.rootDir, self.prefix, 'submissions').as_posix()],
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
    for file in self.detector.test_files:
        test_files_student_dict[file.split(self.detector.test_dirs[0])[1].split('/')[1]].append(file)
    
    # create the report directory inside the prefix folder in the root directory.
    reportDir = "Reports"
    Path.home().joinpath(self.rootDir, self.prefix, reportDir).mkdir(parents=True, exist_ok=True)

    print(f"{time.time()-start_time:6.2f}: Beginning code comparison")
    for student, test_files in tqdm(test_files_student_dict.items(), bar_format='   {l_bar}{bar}{r_bar}'):
      result_dict = defaultdict(list)
      studentReportPath = Path.home().joinpath(self.rootDir, self.prefix, reportDir, f"{student}.json")
      # if the report has already been generated, for the student, then skip
      if studentReportPath.exists(): continue
      for test_f in test_files:
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
          overlap, (sim1, sim2), (slices1, slices2) = compare_files(
              self.detector.file_data[test_f], self.detector.file_data[ref_f],
          )
          # if the similarity is greater than the threshold then append
          if sim1 > self.detector.display_t or sim2 > self.detector.display_t:
            # convert {some_file_path}/student1/.... to student1/...
            relative_test_f_path = test_f.split(self.detector.test_dirs[0])[1][1:]
            relative_ref_f_path = ref_f.split(self.detector.ref_dirs[0])[1][1:]
            result_dict[relative_test_f_path].append({
              'ref_file': relative_ref_f_path,
              'overlap': overlap,
              'test_similarity': sim1,
              'ref_similarity': sim2,
              'test_file_slices': slices1,
              'ref_file_slices': slices2
            })
      # print(f'writing to {reportDir}/{student}.json')
      with open(studentReportPath, 'w') as f:
        json.dump(result_dict, f, indent=2, cls=NumpyEncoder)
    print(f"{time.time()-start_time:6.2f}: Code comparison completed")
    # Uploading the files in the reportDir to the bucket
    if not self.silent:
      print(f'Results saved to {Path.home().joinpath(self.rootDir, self.prefix, reportDir)} folder')


  # upload the Reports folder to the bucket
  def upload_reports(self):
    """
    Used to upload the reports to the bucket. This assumes that the reports are
    generated and stored in the Reports folder inside "~/<rootDir>/<prefix>"
    as json files.

    It will upload all the files inside the bucket with the key as
    "<prefix>/Reports/<file_name>".
    """
    # upload the files to the bucket
    report_dir = Path.home().joinpath(self.rootDir, self.prefix, 'Reports')
    # walk through the report directory and get all the report files
    report_files = [f for f in report_dir.rglob('*.json')]
    if len(report_files) == 0:
      print("No reports in the {} folder.".format(report_dir))
      return
    if not self.silent:
      print(f"Uploading {len(report_files)} reports to the bucket")
    
    start_time = time.time()
    print("\n  0.00: Uploading reports to the bucket")
    for report_file in tqdm(report_files, bar_format='   {l_bar}{bar}{r_bar}'):
      s3_key = "{}/Reports/{}".format(self.prefix, report_file.name)
      self.bucket.upload_file(report_file, s3_key)
    print(f"{time.time()-start_time:6.2f}: Reports uploaded to the bucket")


  def clean_up(self):
    """
    Used to cleanup the resources like deleting files etc.. But for now we are just
    closing the active s3 connection.
    """
    
    # closing the s3 connection 
    if self.detector is not None:
      print('\nClosing the s3 connection...', end=' ')
      self.bucket.meta.client._endpoint.http_session.close()
      print('Done', end='\n\n')
      

