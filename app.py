import os
import sys

from dotenv import load_dotenv

from S3Helper import S3Helper

load_dotenv()

aws_bucket_name = os.getenv('AWS_BUCKET_NAME')
aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_secret_key = os.getenv('AWS_SECRET_KEY')

# print(sys.argv)

directories = []
if len(sys.argv) == 2 and len(sys.argv[1].strip()) > 0:
    directories.append(sys.argv[1])
else:
    directory1 = '/Users/rcravens/Google Drive/My Drive/__youtube/2024_Q1/2024-02-17-slim-truck-cap'
    directories.append(directory1)
    directory2 = '/Users/rcravens/Google Drive/My Drive/__youtube/2024_Q1/2024-02-19-django-docker-starter'
    directories.append(directory2)

helper = S3Helper(aws_bucket_name, aws_access_key, aws_secret_key)
helper.add_directory_map('/Users/rcravens/Google Drive/My Drive/__youtube', 'youtube')
helper.add_directory_map('/Users/rcravens/Dropbox/_youtube', 'youtube')

for directory in directories:
    helper.archive_directory(directory)

    if helper.is_archive_valid(directory, use_e_tag=True):
        print('Archive is valid')
    else:
        print('Archive is not valid')
