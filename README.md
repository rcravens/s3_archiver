# S3 Archiver

This is python application to archive a directory to an S3 bucket.

## Getting Started

### Configuration Settings

Copy `.env_example` to `.env` and set the following variables:

#### Application Settings

- `AWS_BUCKET_NAME` The S3 bucket name.
- `AWS_ACCESS_KEY` - Access key issued to the IAM user who has full access to the bucket
- `AWS_SECRET_KEY` - Secret key for the above access key

## Usage

To run the application:

- `python app.py <directory to archive>`

