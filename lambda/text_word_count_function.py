import json
import boto3
from datetime import datetime

s3 = boto3.client('s3')
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

SNS_TOPIC_ARN = 'arn:aws:sns:eu-west-3:668679959471:TextCount'  # Replace
DDB_TABLE_NAME = 'Text-count-table'  # Replace with your table name

def lambda_handler(event, context):
    try:
        # Get bucket and file info
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        # Download file to /tmp
        download_path = f'/tmp/{key}'
        s3.download_file(bucket, key, download_path)
        
        # Count words
        with open(download_path, 'r') as f:
            text = f.read()
            word_count = len(text.split())
        
        # Save result to processed bucket
        result_key = f'wordcount-{key}'
        result_body = f'Total words in {key}: {word_count}'
        s3.put_object(
            Bucket='text-count-processed-bucket',  # Replace
            Key=result_key,
            Body=result_body
        )
        
        # Save metadata to DynamoDB
        table = dynamodb.Table(DDB_TABLE_NAME)
        table.put_item(
            Item={
                'Filecount': key, # Replace with correct key name
                'WordCount': word_count,
                'ProcessedAt': datetime.utcnow().isoformat()
            }
        )
        
        # Publish SNS notification
        message = f'File {key} processed successfully! {word_count} words counted.'
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject='Text Processing Complete'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(message)
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }
