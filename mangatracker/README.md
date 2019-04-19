# MangaTracker Service
Tracks manga by scraping TenManga with a Lambda triggered by CloudWatch. Several more Lambdas process the data, after which it is saved to a DynamoDB table. Updates to the table trigger a Lambda (via DynamoDB Streams) that then sends a message to a SQS queue, which is consumed by SNS, which sends me a text.

## Functions
MangaTracker utilizes the following Lambda functions.

### Scraper
This Lambda scrapes TenManga. It is triggered by a recurring, timed CloudWatch event.

### Process
This Lambda performs file processing steps - at present, there are none. I may add compression at a later time, but for now, no processing is needed, so all this step does is copy the file from the "raw" bucket to the "processed" bucket.

### Transform
This Lambda performs the data transformations required to turn the processed file (which is still HTML) into a dictionary object.

### Insert
This Lambda checks for new data in the scraped data that isn't contained in my DynamoDB table. If new data is found, it inserts those rows.  
