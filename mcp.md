We now need to add an MCP server that will run in a separate container having access to the database and the filesystem.

* There shall be a user with auth for each user in mrdocument.
* The MCP server shall only have access to the records the user has access to.
* It shall have a find method allowing to do complex searches on context, metadata, tags, filenames, description, summary and content.
* It shall be possible to set which columns to search on, logically connecting different conditions.
* Inside the metadata JSON, it shall be possible to search for specific keys/values.
* * Is there any common DSL to specify such a search?
* Search shall return the record without the content, summary.
* There shall be a method to return the content of a record.
* There shall be a method to return the summary of a record.
