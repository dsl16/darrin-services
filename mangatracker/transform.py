# TRANSFORMATION LAMBDA - TRANSFORMS INGESTED FILES INTO DFS
# https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html
#     Doc for using AWS Lambda with an S3 event as a trigger
import os
import ast
import time
import json
import boto3
import logging
from bs4 import BeautifulSoup

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3',region_name='us-east-2')

def to_df(event,context):
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    filename_from_event = event['Records'][0]['s3']['object']['key']
    file = filename_from_event.split('/')[1]
    target_bucket = 'darrin-testing'

    s3.Bucket(source_bucket).download_file(filename_from_event,file)
    data = file_to_dict(file)

    filename = filename_from_event.replace('processed','parsed')
    send_dict_to_s3(filename,data,target_bucket)

    body = '{filename} saved to {bucket}'.format(filename=filename,
                                                 bucket=target_bucket)
    logging.info(body)
    return {
        "statusCode": 200,
        "body": body
    }

def send_dict_to_s3(filename,data,bucket):
    import_time = time.strftime("%Y-%m-%d_%H_%M_%S")
    with open(filename,'w') as outfile:
        json.dump(data,outfile)
    s3.Bucket(bucket).put_object(Key=filename,Body=open(filename,'rb'))
    os.remove(filename)
    return True

def data_lengths_test(data):
    if len(data['chapter']) == len(data['date_uploaded']) == len(data['url']):
        return True
    else:
        raise ValueError('Data Column Lengths are not equal')

# Download and transform the file to a df
def file_to_dict(filename_from_event):
    # Read in data
    with open(filename_from_event,'r') as data:
        data = ast.literal_eval(data.read())
    os.remove(filename_from_event)
    manga_name = data[0]
    chapter_list_raw = BeautifulSoup(data[1], 'html.parser')

    # Parse for chapter information
    chapter_links = []; chapter_names = []; date_uploads = []
    for chapter in chapter_list_raw.find_all('li',{'class':None}):
        chapter_links.append(chapter.find('div',{'class': 'chapter-name short'}).a.get('href'))
        date_uploads.append(chapter.find('div',{'class': 'add-time page-hidden'}).text)
        chapter_name_parts = chapter.find('div',{'class': 'chapter-name short'}).text.split(' ')
        if chapter_name_parts[-1].strip()[-3:] == 'new':
            chapter_names.append(chapter_name_parts[-1].strip()[:-3])
        else:
            chapter_names.append(chapter_name_parts[-1])

    # Create dict and send data
    data = {'title':manga_name,
            'chapter':chapter_names,
            'date_uploaded':date_uploads,
            'url':chapter_links}
    logger.info('{} parsed'.format(manga_name))

    # Run Unit Tests
    data_lengths_test(data)

    return data
