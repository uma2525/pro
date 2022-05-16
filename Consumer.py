#!/usr/bin/python3
import boto3
import time
import json
import decimal
 
# Kinesis setup
kinesis = boto3.client("kinesis", region_name="us-east-1")
shard_id = "shardId-000000000000"
pre_shard_it = kinesis.get_shard_iterator(
    StreamName='CadabraOrders',
    ShardId=shard_id,
    ShardIteratorType='LATEST'
)
shard_it = pre_shard_it["ShardIterator"]
 
# S3 setup
s3 = boto3.resource('s3')
myobject = s3.Object('asign25', 'assignment1.json')

# Initializing empty bytearray
response = bytearray() 
        
while 1==1:
        
        out = kinesis.get_records(ShardIterator=shard_it, Limit=100)
        for i in range(len(out['Records'])):
            # Appending data to response variable
            response.extend(out['Records'][i]['Data'])
            
        print(response)
    
        # Transferring data to S3
        myobject.put(Body=response)
        
        # Will exit loop once data has been transferred
        if response != b'':
            break
 
        shard_it = out["NextShardIterator"]
        time.sleep(1.0)
