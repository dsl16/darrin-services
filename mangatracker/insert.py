# COMPARISON LAMBDA - ADD NEW LINES TO DYNAMODB TABLE
import os
import time
import json
import boto3
import logging
from decimal import Decimal, InvalidOperation
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Connect to S3 & DynamoDB table
s3 = boto3.resource('s3',region_name='us-east-2')
dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
table = dynamodb.Table('manga_chapters')

def to_dynamodb(event,context):
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    filename_from_event = event['Records'][0]['s3']['object']['key']

    s3.Bucket(source_bucket).download_file(filename_from_event,filename_from_event)
    logging.info('{filename} downloaded'.format(filename=filename_from_event))
    body = insert_chapters(filename_from_event)
    
    return {
        "statusCode": 200,
        "body": body
    }

def ix_to_remove(data,response):
    new_chapter_list = data['chapter']
    new_date_uploaded_list = data['date_uploaded']
    new_url_list = data['url']
    remove_ix = []
    for item in response['Items']:
        try:
            remove_ix.append(new_chapter_list.index(str(item['chapter'])))
        except ValueError:
            # If can't find the item, it's new
            pass
    return remove_ix

# Download file that was just added and load data
def insert_chapters(filename_from_event):
    with open(filename_from_event) as json_file:
        data = json.load(json_file)
    os.remove(filename_from_event)

    # Query the DynamoDB table for that title
    title = data['title']
    response = table.query(TableName='manga_chapters',
                           KeyConditionExpression=Key('title').eq(title))

    # If no records at present, insert all records as new
    if response['Count'] == 0:
        logger.info('Ingesting a whole new manga: {}'.format(title))
        # Need to add all items
        cnt = 0
        for i in range(len(data['chapter'])):
            try:
                table.put_item(
                    Item = {
                        'chapter': Decimal(data['chapter'][i]),
                        'date_ingested': time.strftime("%Y-%m-%d"),
                        'date_uploaded': data['date_uploaded'][i],
                        'title': title,
                        'url': data['url'][i]
                    }
                )
                cnt += 1
            except InvalidOperation:
                logger.info('{} not added.'.format(data['chapter'][i]))
    # If records exist, only insert new records
    else:
        logger.info('There are existing chapters for {}'.format(title))
        # Need to compare chapter numbers
        remove_ix = ix_to_remove(data,response)

        for index in sorted(remove_ix,reverse=True):
            for my_list in [data['chapter'],data['date_uploaded'],data['url']]:
                del my_list[index]
        cnt = 0
        for i in range(len(data['chapter'])):
            try:
                table.put_item(
                    Item = {
                        'title': title,
                        'chapter': Decimal(data['chapter'][i]),
                        'date_ingested': time.strftime("%Y-%m-%d"),
                        'date_uploaded': data['date_uploaded'][i],
                        'url': data['url'][i]
                    }
                )
                cnt += 1
            except InvalidOperation:
                logger.info('{} not added.'.format(data['chapter'][i]))
    body = '{cnt} new chapters added to {title}.'.format(cnt=cnt,
                                                          title=title)
    logger.info(body)
    return body
