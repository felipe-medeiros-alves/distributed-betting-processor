#!/bin/sh
set -e
awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo --attributes FifoQueue=true
DLQ_URL=$(awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo --query QueueUrl --output text)
DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes "FifoQueue=true,ContentBasedDeduplication=false,VisibilityTimeout=30,RedrivePolicy={\"deadLetterTargetArn\":\"$DLQ_ARN\",\"maxReceiveCount\":\"5\"}"
awslocal sqs create-queue --queue-name wallet-events.fifo --attributes FifoQueue=true,ContentBasedDeduplication=false
