# Document Reader

Parses any kind of tabular data

## Setup parse

* Add document reader concern to model to enable parsing features.
* Call ```document_definitions``` class function within your model with available fields schema definition

## Parsing parse

* Upload document to ```source``` field
* First you need to call ```analyze``` to let document reader extract data from XLS or archived file to CSV and determine CSV type and fields
* Analyzed schema will be saved to ```parse_definition``` attribute of your model
* Check or modify schema and then use it in your data import function

