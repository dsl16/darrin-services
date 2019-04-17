# MangaTracker Service
Tracks manga by scraping TenManga with a Lambda triggered by CloudWatch. Several more Lambdas process the data, after which it is saved to a DynamoDB table. Updates to the table trigger a Lambda (via DynamoDB Streams) that then sends a message to a SQS queue, which is consumed by SNS, which sends me a text.
