from datetime import datetime
from google.cloud import storage
import kfp
from google.cloud import aiplatform
import pandas as pd
import json
import gcsfs

def create_batch_prediction_job(project: str, location: str, model_resource_name: str, job_display_name: str, gcs_source , gcs_destination: str, sync: bool = True):
          aiplatform.init(project='mtk-summer-internship', location=location)
          my_model = aiplatform.Model(model_resource_name)
          batch_prediction_job = my_model.batch_predict(job_display_name=job_display_name, gcs_source=gcs_source, gcs_destination_prefix=gcs_destination, sync=sync)

def reformat_classification(df, blob):
     for index, row in df.iterrows():
          with open(f'/tmp/{row["File_id"]}.txt', 'w') as f:
               f.write(row['content'])
          df.at[index,'content'] = f'gs://mtk-storage-bucket/classification-txtfiles/{row["File_id"]}.txt'
     df = df[['content']]
     df.to_json('/tmp/classification_prediction_data.jsonl', orient = 'records', lines = True)
     blob.upload_from_filename('/tmp/classification_prediction_data.jsonl')
     fs = gcsfs.GCSFileSystem()
     fs.put('/tmp/*', 'gs://mtk-storage-bucket/classification-txtfiles')

def classification_prediction_trigger(event, context):
     """Triggered by a change to a Cloud Storage bucket.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     file = event
     print(f"Processing file: {file['name']}.")

     #Set variables
     client = storage.Client()
     bucket = client.get_bucket('mtk-storage-bucket')
     TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
     REGION = 'us-central1'
     aiplatform.init(project = 'mtk-summer-internship', staging_bucket = 'gs://comment-data-bucket', location = REGION)
     
     #Get data
     cblob = bucket.blob('classification_prediction_data.jsonl')
     path = f'gs://prediction-data-bucket/{file["name"]}'
     classification_df = pd.read_excel(path)
     classification_df = classification_df[['content', 'File_id']]

     #Reformat data and upload to classification-txtfiles
     reformat_classification(classification_df, cblob)

     #Create prediction and store in results bucket
     create_batch_prediction_job(project = 'mtk-summer-internship', location = REGION, 
                                             model_resource_name= '5699811103786139648',
                                             job_display_name= f'classification-prediction-{file["name"][:5]}-{TIMESTAMP}',
                                             gcs_source= 'gs://mtk-storage-bucket/classification_prediction_data.jsonl',
                                             gcs_destination= 'gs://prediction-results-bucket',
                                             sync = True)
     
     return