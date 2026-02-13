import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
from apache_beam.options.value_provider import ValueProvider
from datetime import datetime
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import fileio
import json
import logging

# Configure logging to suppress specific warnings
class WarningFilter(logging.Filter):
    def filter(self, record):
        if record.levelno == logging.WARNING and "No iterator is returned by the process method" in record.getMessage():
            return False
        return True
logger = logging.getLogger("apache_beam.transforms.core")
logger.addFilter(WarningFilter())

class PipeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_file', type=ValueProvider, required=True,help='Input file to process')
        parser.add_argument('--output_table', type=ValueProvider, required=True, help='Output BigQuery table for results')
        parser.add_argument('--schema', type=ValueProvider, help='Schema of the table')
        parser.add_argument('--separator', type=ValueProvider, default=',', help='Separator for csv file')

def run(argv):
    """Main entry point"""
    # init pipeline options
    options = PipelineOptions(argv)


    google_cloud_options= options.view_as(GoogleCloudOptions)
    setup_options= options.view_as(SetupOptions)
    
    runetimeoptions = options.view_as(PipeOptions)
    
    # unchanged (default) options
    google_cloud_options.staging_location = 'gs://dataflow_bucket_processing-452316/staging'
    google_cloud_options.temp_location = 'gs://dataflow_bucket_processing-452316/temp'
    google_cloud_options.region = 'europe-west1'
    setup_options.requirements_file = 'requirements.txt'

    # input and output options needed at runtime
    input_file = runetimeoptions.input_file
    output_table = runetimeoptions.output_table
    schema = runetimeoptions.schema
    separator = runetimeoptions.separator

    # getting schema value and separator at building not until runtime because they are needed immediately
    if isinstance(schema,ValueProvider):
        schema_path = schema.get()
        
    if isinstance(separator,ValueProvider):
        separator = separator.get()
    else:
        separator = ','
    
    # reading table schema
    with open(schema_path,'r') as f:
        table_schema = json.load(f)
    
    # parse  file function
    def parse_file(file_metadata,table_schema=table_schema,separator=separator):
        from datetime import datetime
        file_path = file_metadata.metadata.path
        extension = file_path.split('.')[-1].lower()
        content = file_metadata.read().decode('utf-8')
        

        if extension == "csv" or extension == "txt":
            json_objects = []
            lines =content.splitlines()
            field_names = [field['name'] for field in table_schema['fields']]
            for line in lines[1:]:
                values=  line.split(separator)
                values = [value.replace('"','').replace(",",".") for value in values]
                row = dict(zip(field_names,values))
                row['insert_date'] = datetime.now().isoformat()
                row = json.loads(json.dumps(row))
                yield row

        elif extension.lower() == "json" or extension.lower() == "jsonl":
            json_objects = json.loads(content) if extension.lower() == "json" else [json.loads(line) for line in content.splitlines()]
            for json_object in json_objects:
                yield json_object
        else:
            raise ValueError(f'unsupported file format {extension}')

    
    # pipeline definition
    with beam.Pipeline(options=options) as p:
        (p
        | 'MatchFiles' >> beam.io.fileio.MatchFiles(input_file)
        | 'ReadFiles' >> beam.io.fileio.ReadMatches()
        | 'ProcessAndParseFiles' >> beam.FlatMap(lambda file_metadata: parse_file(file_metadata,table_schema,separator))
        | 'WriteToBigQuery' >> WriteToBigQuery(table=output_table, schema=table_schema, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,method='FILE_LOADS', insert_retry_strategy=beam.io.gcp.bigquery.RetryStrategy.RETRY_ON_TRANSIENT_ERROR)
        )

if __name__=='__main__':
    run()