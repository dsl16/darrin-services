# INITIAL PROCESSING LAMBDA - FOR NOW, DOES NOTHING BUT COPY
# add code to copy the file from the raw bucket to the processed bucket
# you can use this example: https://medium.com/@stephinmon.antony/aws-lambda-with-python-examples-2eb227f5fafe
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def copy(event,context):
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    target_bucket = 'darrin-testing'
    copy_source = {'Bucket': source_bucket,
                   'Key': key}

    try:
        logging.info('Waiting for the file to persist in the source bucket.')
        waiter = s3.get_waiter('object_exists')
        waiter.wait(Bucket=source_bucket, Key=key,
                    WaiterConfig={
                        'Delay': 15,
                        'MaxAttempts': 3
                    })
        logging.info('Copying object from source s3 bucket to target s3 bucket.')
        s3.copy_object(Bucket=target_bucket, Key=key.replace('raw','processed'),
                       CopySource=copy_source)

        return {
            "statusCode": 200,
            "body": 'Successful copy of {key} from {source_bucket} to {target_bucket}.'.format(key=key,
                                                                           source_bucket=source_bucket,
                                                                           target_bucket=target_bucket)
        }
    except Exception as e:
        logging.error(e)
        logging.error('Error getting object {} from bucket {}.'.format(key,source_bucket))
        raise e

        return {
            "statusCode": 500,
            "body": e
        }
