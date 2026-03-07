import json
import boto3
from datetime import datetime

s3 = boto3.client('s3')
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

SNS_TOPIC_ARN = 'arn:aws:sns:eu-west-1:578761488692:EyongFileNotification' # replace
DDB_TABLE_NAME = 'EyongFileMatadata' # replace

def lambda_handler(event, context):
    print(f"Raw event received: {json.dumps(event)}")  # Log full event for debugging

    for sqs_record in event['Records']:
        try:
            # ✅ FIX: SQS wraps the S3 event as a JSON string in 'body'
            sqs_body = json.loads(sqs_record['body'])
            print(f"Parsed SQS body: {json.dumps(sqs_body)}")

            # ✅ FIX: Loop through S3 records inside the SQS message
            for s3_record in sqs_body['Records']:
                bucket = s3_record['s3']['bucket']['name']
                key = s3_record['s3']['object']['key']
                print(f"Processing file: s3://{bucket}/{key}")

                # Download file to /tmp
                download_path = f'/tmp/{key}'
                s3.download_file(bucket, key, download_path)

                # Count words
                with open(download_path, 'r') as f:
                    text = f.read()
                word_count = len(text.split())
                print(f"Word count for {key}: {word_count}")

                # Save result to processed bucket
                result_key = f'wordcount-{key}'
                result_body = f'Total words in {key}: {word_count}'
                s3.put_object(
                    Bucket='eyong-file-processed-bucket', # replace
                    Key=result_key,
                    Body=result_body
                )
                print(f"Result saved to eyong-file-processed-bucket/{result_key}") # replace

                # Save metadata to DynamoDB
                table = dynamodb.Table(DDB_TABLE_NAME)
                table.put_item(
                    Item={
                        'MyFileMetadata': key, # replace
                        'WordCount': word_count,
                        'ProcessedAt': datetime.utcnow().isoformat()
                    }
                )
                print(f"Metadata saved to DynamoDB for {key}")

                # Publish SNS notification
                message = f'File {key} processed successfully! {word_count} words counted.'
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=message,
                    Subject='Text Processing Complete'
                )
                print(f"SNS notification sent for {key}")

        except Exception as e:
            print(f"ERROR processing record: {str(e)}")  # ✅ Error now visible in logs
            raise e  # ✅ Re-raise so SQS retries instead of silently deleting message
