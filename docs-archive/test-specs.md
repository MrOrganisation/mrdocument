We need to create an integration test for @mrdocument that runs against a live instance of the service on a remote host.

# Configuration and Example Data
* There shall be a full example configuration for a test user that can be used on the live instance. It shall contain at least two contexts with distinct different configurations.
* * Each context shall have at least one other metadata field that is reliably determined by the model.
* * Both configurations shall have `folders` spec that has at least a depth of two folders. Only use such fields that are designed to be reliably inferred.
* There shall be a full corresponding syncthing configuration for the client on Ubuntu.
* There shall be a script to generate example PDF, TXT, RTF documents for testing purposes with content making it easy for the AI model to determine the correct context and if possible their metadata.
* * Test data must be such that the model will reliably assign the right context.
* * Test data must be such that the model will reliably determine the correct date.
* There shall be a script to generate example audio files for testing purposes with conversation content making it easy for the AI model to determine the correct context and if possible their metadata:
* * It shall be possible to generate such audio files without an intro providing metadata, with one at the beginning and with one at the end.

# Test Cases
## Documents
For each in `file_type` in PDF, TXT, RTF, each `context` in the example contexts, each document from a subset of example documents designed for `context`:

* Copy document to `incoming`.
* * Verify that an output document appears in `processed`.
* * Verify that the source document is stored in `archive`.
* * Verify that the name of the processed document matches the expectations:
* * * The context must match.
* * * The date must match.
* * * The one metadata field that is specifically designed to be reliably determined must be matched.

* Move the document from `processed` to `reviewed`.
* * Verify that the document is sorted to the correct folder.
* * Verify that the document is linked to the correct smart folders.


For each in `file_type` in PDF, TXT, RTF, each `context` in the example contexts, one document not used in tests so far from `context`:
* Copy document to `sorted/{context}/.`
* * Verify that an output document appears `sorted/{context}/...` at the right location depending on metadata.
* * Verify that the source document is stored in `archive`.
* * Verify that the name of the processed document matches the expectations:
* * * The context must match.
* * * The date must match.
* * * The one metadata field that is specifically designed to be reliably determined must be matched.

For each in `file_type` in PDF, TXT, RTF, each `context` in the example contexts, one document not used in tests so far from the context that is not `context`:
* Copy document to `sorted/{context}/.`
* * Verify that an output document appears `sorted/{context}/...` somewhere. We cannot control that metadata is correctly inferred because we force a document into a wrong context.
* * Verify that the source document is stored in `archive`.
* * Verify that the name of the processed document matches the expectations:
* * * The context must match.
* * * The date must match.

For each audio file with intro (both at the beginning and at the end):
* Copy file to `incoming`.
* * Verify that an output TXT document appears in `processed`.
* * Verify that the source audio file is stored in `archive`.
* * Verify that the name of the output TXT document matches the expectations:
* * * The context must match.
* * * The date must match.
* * * The one metadata field that is specifically designed to be reliably determined must be matched.
* * Verify that the intro is found at the beginning of the TXT document. (Use some sort of fuzzy matching.)

For each audio file without intro:
* Copy file to `incoming`.
* * Verify that an output TXT document appears in `processed`.
* * Verify that the source audio file is stored in `archive`.
* * Verify that the name of the output TXT document matches the expectations:
* * * The context must match.
* * * The date must match.
* * * The one metadata field that is specifically designed to be reliably determined must be matched.
