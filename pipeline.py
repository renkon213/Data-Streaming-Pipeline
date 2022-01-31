from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

import apache_beam as beam
import argparse

# Project_id, subscription, and bigquery schema
PROJECT_ID = 'PROJECT_ID'
SUBSCRIPTION = 'projects/' + PROJECT_ID + '/subscriptions/SUBSCRIPTION_NAME'
SCHEMA = 'user_id:STRING,device:STRING,page:STRING,timestamp:TIMESTAMP'


def parse_pubsub(data):
    import json
    return json.loads(data)

def fix_timestamp(data):
    import datetime
    d = datetime.datetime.strptime(data['timestamp'], "%d/%b/%Y:%H:%M:%S")
    data['timestamp'] = d.strftime("%Y-%m-%d %H:%M:%S")
    return data

def fix_device_name(data):
    if int(data['device']) >= 1024:
        data['device'] = 'desktop'
    elif int(data['device']) >= 768:
        data['device'] = 'tablet'
    elif int(data['device']) < 768:
        data['device'] = 'smartphone'
    else:
        data['device'] = 'others'
    
    return data


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    # Create pipeline
    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION).with_output_types(bytes)
       | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
       | 'PubSubToJSON' >> beam.Map(parse_pubsub)
       | 'FixTimestamp' >> beam.Map(fix_timestamp)
       | 'FixDeviceName' >> beam.Map(fix_device_name)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:YOUR_BIGQUERY_TABLE'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
    result.wait_until_finish()
