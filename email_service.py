
import logging
import boto3
import uuid
import time
import json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# DynamoDB configuration
dynamodb = boto3.resource('dynamodb', region_name = 'us-east-1', endpoint_url = "http://dynamodb.us-east-1.amazonaws.com")
table = dynamodb.Table("csye6225")
key_name = 'emailId'
key_link = 'link'
key_ttl = 'expirationTime'
TTL = 60 * 60 # seconds

# email configuration
AWS_REGION = "us-east-1"
SUBJECT = "Due Bills"
CHAR_SET = "UTF-8"
BODY_TEXT = "Here is the due for next X days."
BODY_HTML = """<html>
<head></head>
<body>
    <h4>Due Bills</h4>
"""
client = boto3.client("ses", region_name=AWS_REGION)

# logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("Loading function...")

def email_handler(event, context):
    '''handler for aws lambda'''
    # get message from SNS
    try:
        message = event['Records'][0]['Sns']['Message']
        print("From SNS (before decoding):" + message)
        print(type(message))
        message = message.replace("\n\r","")
        dic = json.loads(message, strict=False)
        print("From SNS (after decoding):" + message)
        email_address = dic['ownerEmail']  # receiver's email address
        record_id = dic['recordId']  # record id for all due bills
        domain = dic['domain'] # set the link that receiver can get the due bills
        link = domain + "/v1/bills/" + record_id
    except Exception:
        logger.error("Parse SNS message error, do nothing and exit")
        return None

    print("Set sender domain: [ " + domain + " ] and get email: [ " + email_address + " ]")
    
    if email_exists(email_address):
        if not token_expired(email_address):
            logger.warning("Email [ " + email_address + " ], token not expired")
            return None
  
    # set record id as the token in dynamo db
    save_item(email_address, record_id)
    logger.info("Email token saved to DynamoDB")
    send_email(email_address, domain, link)

    return None

def token_expired(email):
    '''true if token expired'''
    try:
        response = table.get_item(
            Key={
                key_name: email
            }
        )
    except ClientError:
        logger.error("fetch response failed, key name error in [ token_expired ]")
        exit(1)
        return False
    else:
        try:
            expiration_time = response['Item'][key_ttl] # the time to live timestamp
        except KeyError:
            logger.error("key error in querying timstamp of email [ " + email + "]  in [ token_expired ]")
            exit(1)
            return False
        else:
            curr = int(time.time())
            if curr >= expiration_time:
                return True
            gap =  (expiration_time - curr) / 60
            print("Still has: " + str(TTL - int(gap)) + " minutes")
            return False
    
def email_exists(email):
    '''true if email record still in db'''
    try:
        response = table.get_item(
            Key={
                key_name: email
            }
        )
    except ClientError:
        logger.error("fetch response failed, key name error in [ email_exists ]")
        exit(1)
        return False
    else:
        if 'Item' not in response:
            return False
        else:
            return True

def save_item(email, link):
    '''save item in db'''
    if not email_exists(email):
        # create item
        try:
            response = table.put_item(
                Item={
                    key_name: email,
                    key_link: link,
                    key_ttl: int(time.time()) + TTL
                }
            )
        except ClientError as e:
            logger.error("Error create item in [ save_item ]")
            exit(1)
            return
    else:
        # item exits in database, token expired
        try:
            response = table.update_item(
                Key={
                    key_name: email
                },
                UpdateExpression="set "+key_link+"=:to, "+key_ttl+"=:ti",
                ExpressionAttributeValues={
                    ':to': link,
                    ':ti': int(time.time()) + TTL
                },
                ReturnValues="UPDATED_NEW"
            )
        except:
            logger.error("Error save item through updating in [ save_item ]")
        else:
            # logger.info("Email still in database, token not expired or not deleted")
            return


def send_email(email, domain, link):
    '''send email with password reset token'''
    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    email,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHAR_SET,
                        'Data': BODY_HTML + "<p>" + BODY_TEXT + "<br/><br/>http://" + link + "</p></body></html>",
                    },
                    'Text': {
                        'Charset': CHAR_SET,
                        'Data': BODY_TEXT + "\nhttp://" + link,
                    },
                },
                'Subject': {
                    'Charset': CHAR_SET,
                    'Data': SUBJECT,
                },
            },
            Source="noreply@" + domain,
            # ConfigurationSetName=CONFIGURATION_SET,
        )
    except Exception as e:
        logger.error("Send email to receipient failed in [ send_email ]")
        logger.error(str(e))
        exit(1)
        return
    else:
        logger.info("Email sent, Message ID: [ " + response['MessageId'] + " ]")
        return

