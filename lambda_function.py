import json 
import pymysql
import os
import sys
import logging

from decimal import Decimal

print('Loading function sophia')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration Values
endpoint = os.environ['db_endpoint']
username = os.environ['db_admin_user']
password = os.environ['db_admin_password']
db_name = "configuration"

dynamo_table = "budgets" 

def lambda_handler(event,environment):
    
    print("Handling MODIFY Event BUDGET")


    try:
   	    conn = pymysql.connect(host=endpoint, user=username, passwd=password, db=db_name, connect_timeout=5)
    except pymysql.MySQLError as e:
       logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
       logger.error(e)
       sys.exit()

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    for record in event['Records']:
        print(record)
        print(campaign_id:=record['dynamodb']['Keys']['campaign_id']['S'])
        print(balance:=record['dynamodb']['NewImage']['balance']['N'])
        print(budget:=record['dynamodb']['NewImage']['budget']['N'])
        print(Decimal(balance)>= Decimal(budget))
        
        if((Decimal(balance)>= Decimal(budget)) and Decimal(budget)!= -1):
            cursor = conn.cursor()
            query = "UPDATE advertiser_campaigns SET status=0  WHERE id= %s"
            cursor.execute(query, int(campaign_id))
        

