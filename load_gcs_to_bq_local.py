import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
from datetime import datetime
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import fileio
import json
import argparse
import logging

# Configure logging to suppress specific warnings
class WarningFilter(logging.Filter):
    def filter(self, record):
        if record.levelno == logging.WARNING and "No iterator is returned by the process method" in record.getMessage():
            return False
        return True
logger = logging.getLogger("apache_beam.transforms.core")
logger.addFilter(WarningFilter())


def run(argv=None):
    """Main entry point"""
    # initialize parser
    parser = argparse.ArgumentParser()

    # add arguments
    parser.add_argument('--input_file', dest='input_file', required=True,help='Input file to process')
    parser.add_argument('--output_table', dest='output_table', required=True, help='Output BigQuery table for results')
    parser.add_argument('--project', dest='project', required=True, help='GCP project to run the dataflow job')
    parser.add_argument('--service_account_email', dest='service_account_email', help='Service account email to run the dataflow job')
    parser.add_argument('--runner', dest='runner', default='DataflowRunner', help='Runner for the pipeline')
    parser.add_argument('-- job_name', dest='job_name', default='gcstobigquery', help='Name of the job')
    parser.add_argument('--schema', dest='schema', help='Schema of the table')
    parser.add_argument('--separator', dest='separator', default=',', help='Separator for csv file')
    # parse entred arguments
    known_args, pipeline_args = parser.parse_known_args(argv)

    # initialize pipeline options
    options = PipelineOptions(pipeline_args)
    google_cloud_options= options.view_as(GoogleCloudOptions)
    standard_options = options.view_as(StandardOptions)
    setup_options= options.view_as(SetupOptions)
    
    # runner options
    standard_options.runner = known_args.runner

    # google cloud options
    google_cloud_options.project = known_args.project
    google_cloud_options.service_account_email = known_args.service_account_email
    
    # unchanged options
    google_cloud_options.staging_location = 'gs://dataflow_bucket_processing-452316/staging'
    google_cloud_options.temp_location = 'gs://dataflow_bucket_processing-452316/temp'
    google_cloud_options.region = 'europe-west1'
    setup_options.requirements_file = 'requirements.txt'

    # input and output options
    input_file = known_args.input_file
    output_table = known_args.output_table
    schema = known_args.schema
    separator = known_args.separator

    
    # reading table schema
    with open(schema,'r') as f:
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