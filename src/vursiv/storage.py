import requests
import boto3
import os

class FileStore:
    def __init__(self, root):
        self.root = root
        
    def write(self, key, data):
        path = os.path.join(self.root, key)
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
            
        with open(path, 'w') as outfile:
            outfile.write(data)
            
    def read(self, key):
        path = os.path.join(self.root, key)
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            return None
           
        with open(path, 'r') as outfile:
            return outfile.read()
        
    
class S3InstrumentStore:
    def __init__(self, access_key, secret_key, bucket_name):
        self.s3 = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self.bucket_name = bucket_name
        self.bucket = self.s3.Bucket(self.bucket_name)
        
    def write(self, key, data):
        return self.bucket.put_object(Key=key, Body=data.encode('utf-8')) 
        
    def read(self, key):
        object = self.s3.Object(Bucket=self.bucket_name, Key=key)
        return object.get()['Body'].read().decode('utf-8') 