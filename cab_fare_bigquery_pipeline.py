import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

class AgeRangeTransform(beam.DoFn):
    def process(self, element):
        age = int(element['age'])
        if 18 <= age <= 25:
            element['age_range'] = '18-25'
        elif 26 <= age <= 35:
            element['age_range'] = '26-35'
        elif 36 <= age <= 45:
            element['age_range'] = '36-45'
        elif 46 <= age <= 55:
            element['age_range'] = '46-55'
        else:
            element['age_range'] = '56+'
        return [element]

class AggregateFareTransform(beam.DoFn):
    def process(self, element):
        key = (element['age_range'], element['state'])
        fare = float(element['fare'])
        return [(key, fare)]


def run_pipeline():
    
    headers = ['date', 'name', 'age', 'city', 'state', 'fare', 'gender', 'occupation']
    
    table_schema = {
        'fields': [
            {'name': 'age_range', 'type': 'STRING'},
            {'name': 'state', 'type': 'STRING'},
            {'name': 'total_fare', 'type': 'FLOAT'}
        ]
    }

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'YOUR_PROJECT_ID'  # Replace with your actual project ID
    google_cloud_options.job_name = 'JOB_NAME'  # Replace with your desired job name
    google_cloud_options.staging_location = 'gs://YOUR_STAGING_LOCATION'  # Replace with your GCS staging bucket
    google_cloud_options.temp_location = 'gs://YOUR_TEMP_FILES_LOCATION'  # Replace with your GCS Temporary files bucket


    with beam.Pipeline(options=options) as pipeline:
        # Read from the input CSV file
        lines = pipeline | 'ReadFromCSV' >> ReadFromText('gs://YOUR_BUCKET/example_cab_data.csv', skip_header_lines=1)

        # Parse CSV lines into dictionaries
        parsed_data = (lines
                       | 'ParseCSV' >> beam.Map(lambda line: dict(zip(headers, line.split(',')))))

        # Apply age range transformation
        age_ranged_data = (parsed_data
                           | 'AgeRangeTransform' >> beam.ParDo(AgeRangeTransform()))

        # Aggregate fare by age range and state
        aggregated_data = (age_ranged_data
                           | 'AggregateFareTransform' >> beam.ParDo(AggregateFareTransform())
                           | 'SumFare' >> beam.CombinePerKey(sum))

        # Write the results to BigQuery
        _ = (aggregated_data
             | 'FormatOutput' >> beam.Map(lambda element: {
                 'age_range': element[0][0],
                 'state': element[0][1],
                 'total_fare': round(element[1], 2)
             })
            | 'WriteToBigQuery' >> WriteToBigQuery(
            table='PROJECT_ID:DATASET_NAME.TABLE_NAME',
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
            )
                 
run_pipeline()
