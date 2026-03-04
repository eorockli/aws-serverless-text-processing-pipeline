# AWS Serverless Text Processing Pipeline

## Overview

This project demonstrates a fully event-driven serverless architecture built on AWS.

When a `.txt` file is uploaded to an S3bucket:

* AWS Lambda is triggered automatically
* The file is processed and words are counted
* Metadata is stored in DynamoDB
* A processed result file is written to a separate S3 bucket
* An SNS notification is sent
* Delete events trigger additional notifications

## Architecture

User → S3 Upload Bucket → Lambda →
• DynamoDB (store metadata)
• S3 Processed Bucket (store result file)
• SNS (email notifications)

Processed bucket delete events → SNS alert
![Architecture Diagram](architecture.png)

## AWS Services Used

* Amazon S3
* AWS Lambda
* Amazon DynamoDB
* Amazon SNS
* Amazon CloudWatch Logs

## Region

eu-north-1 (Stockholm)

## Key Features

* Fully serverless architecture
* Event-driven processing
* Automatic metadata storage
* Notification system
* Logging and monitoring
* Separate upload and processed buckets

## Future Improvements

* Add SQS for decoupling
* Add API Gateway for metadata queries
* Implement IAM least-privilege policies
* Infrastructure as Code (Terraform or CloudFormation)
