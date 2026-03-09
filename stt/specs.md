We are implementing a tool to upload audio files to Google's Speech-To-Text endpoint and retrieve transcripts.

* The project shall be in Python.
* It shall use poetry for package management.
* Documentation for Google's Python module can be found here: https://docs.cloud.google.com/python/docs/reference/speech/latest
* Processing shall be done asynchronously.
* The CLI shall allow uploading a file for transcription, viewing current ongoing operations and retrieving the transcript when an operation
  finishes.
* It may be neccessary to upload the audio files to a Google Cloud Storage. Such storage is available.
* It shall be possible to enabled/disable diarization, timestamps etc.
* It shall be possible to set the language. Default shall be German.

There are a number of questions to clarify before starting the implementation:

* Audio files will may be quite long. What is the longest file accepted by Google? Is it possible to extend this?
* What audio formats are supported?
* What options do exist to enhance audio quality of a recording before sending it to the API?
