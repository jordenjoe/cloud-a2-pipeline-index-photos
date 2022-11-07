import json
import urllib.parse
import boto3
import requests
from requests_aws4auth import AWS4Auth

def indexOpenSearch(document):
    region = 'us-east-1'
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    host = 'https://search-photos-wwyb2fbtpbgr52ygjjbjtky56a.us-east-1.es.amazonaws.com'
    index = 'photos'
    url = host + '/' + index + '/_doc'
        
    headers = { "Content-Type": "application/json" }
    
    r = requests.post(url, auth=awsauth, data=json.dumps(document), headers=headers)

    return r
        
def lambda_handler(event, context):
    
    #this will be our JSON object we store in Opensearch
    A1 = {}
    
    print("event: ",event)
    
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    #start building out A1
    A1["objectKey"] = key
    A1["bucket"]= bucket
    A1["createdTimestamp"] = event['Records'][0]['eventTime']
    A1["labels"] = []
    
    rekognitionClient = boto3.client("rekognition")
    s3Client = boto3.client('s3')
    
    #right now this isn't pulling a x-amz-meta-customLabels because we haven't set it up
    metadata = s3Client.head_object(Bucket = bucket, Key = key)
    print("metadata: ", metadata)

    customLabels = []
    
    try:
        #get custom labels if they exist
        customLabels = metadata["Metadata"]['customlabels']
        print('customLabels: ', customLabels)
        customLabels = customLabels.split(", ")
        print('customLabels: ', customLabels)
    except:
        print("no custom labels")
    
    response = rekognitionClient.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}}, MinConfidence=80)
    print("response: \n\n",response)
    
    #parse response to get labels
    for item in response['Labels']:
        A1["labels"].append(item['Name'])

    for item in customLabels:
        A1["labels"].append(item)

    print('A1: ', A1)
    # A1 = json.dumps(A1)
    
    r = indexOpenSearch(A1)
    print(r.status_code, r.content)
    
    return{
        'statusCode':200,
        'body': json.dumps('Hello!')
    }