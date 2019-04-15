# darrin-services
A monorepo for all my Serverless applications and services.
The rationale for a monorepo [can be found here](https://serverless-stack.com/chapters/organizing-serverless-projects.html).

## mangatracker
A system built on AWS infrastructure with Lambdas, DynamoDB, SQS, and SNS that scrapes TenManga for new updates to manga I want to track. The system should eventually integrate into a habit system as a reward - completion of a habit will unlock a chapter.
