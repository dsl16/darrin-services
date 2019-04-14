# INGEST LAMBDA - SCRAPES THE SITE
import os
import time
import boto3
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')

def scrape(event, context):
    manga_list = ['http://www.tenmanga.com/book/KINGDOM.html',
                  'http://www.tenmanga.com/book/Hardcore+Leveling+Warrior']
    raw_bucket = 'darrin-testing'

    scrape_manga(manga_list,raw_bucket)
    body='Manga list scraped and sent to {bucket}.'.format(bucket=raw_bucket)
    logging.info(body)

    return {
        "statusCode": 200,
        "body": body
    }

def send_txt_to_s3(filename,data,bucket):
    with open(filename,'w') as file:
        file.write(str(data))
    subfolder = 'test-mangatracker-raw'
    s3.Bucket(bucket).put_object(Key='{subfolder}/{filename}'.format(subfolder=subfolder,
                                                                     filename=filename),
                                 Body=open(filename,'rb'))
    logging.info('{filename} saved to S3.'.format(filename=filename))
    os.remove(filename)
    return True

def scrape_manga(manga_list,raw_bucket):
    for manga in manga_list:
        r = requests.get(manga)
        soup = BeautifulSoup(r.text, 'html.parser')

        # Add code to extract the manga name
        manga_names = [soup.find_all('div',{'class': 'book-info'})[0].find('h1').text]

        # Add code to extract the raw list of chapters
        chapter_list_raw = soup.find_all('ul',{'class': 'chapter-box'})

        # Send the raw chapter list and manga names to S3
        manga = manga_names[0].replace(' ','_')
        import_time = time.strftime("%Y-%m-%d_%H_%M_%S")
        filename = '_'.join([manga,'raw',import_time]) + '.txt'
        data = manga_names + [str(chapter_list_raw[0])]
        send_txt_to_s3(filename,data,raw_bucket)
    return True
