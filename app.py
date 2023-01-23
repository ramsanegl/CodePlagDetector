from flask import Flask, request, make_response, jsonify
from CodePlagDetector import CodePlagiarismDetector, defaults

from pathlib import Path

app = Flask(__name__)


@app.route('/hello')
def index():
  return 'Hello !! :)'


@app.route('/api/v1/plagiarism_checker', methods=['POST'])
def plagiarism_checker():
  """
  This function will be called when the user will hit the /plagiarism_checker
  endpoint with a POST request
  
  SAMPLE REQUEST BODY:
  --------------------
    {
    "bucket": "bucket_name",  # Name of the bucket where the files are stored
    "prefix": "prefix_name",  # Prefix of the files in the bucket where boilerplate and
                              # student code files are stored
    }
  
  few other opointsional fields are: extensions, noise_t, guarantee_t, same_name_only, display_t
  Please refer to the CodePlagiarismDetector class for more details.
  
  SAMPLE RESPONSE:
  ----------------
    {
      "status": "success",
      "message": "Plagiarism check completed successfully",
    }
  """
  print('\n*****************************************************************')
  # Get the data from the POST request.
  data = request.get_json(force=True)
  # check for mandatory fields
  if 'bucket' not in data or 'prefix' not in data:
    return make_response({'status': 'Error', 'message': 'bucket and prefix are mandatory fields'}, 400)

  cp_detector = None
  try:
    cp_detector = CodePlagiarismDetector(
      bucket_name=data.get('bucket'),
      prefix=data.get('prefix'),
      extensions=data.get('extensions', defaults.EXTENSIONS),
      noise_t=data.get('noise_t', defaults.NOISE_THRESHOLD),
      guarantee_t=data.get('guarantee_t', defaults.GUARANTEE_THRESHOLD),
      same_name_only=data.get('same_name_only', defaults.SAME_NAME_ONLY),
      display_t = data.get('display_t', defaults.DISPLAY_THRESHOLD),
    )
    cp_detector.initialize()
    cp_detector.run()

  except Exception as e:  # later we can make use of specific exceptions if needed
    return make_response({'status': 'Error', 'message': str(e)}, 400)
  finally:
    if cp_detector is not None: cp_detector.clean_up()
    print('*****************************************************************\n')

  return make_response({'status': 'success', 'message': 'Plagiarism check completed successfully'}, 200)



if __name__ == '__main__':
  app.run(host="0.0.0.0", port=5000)