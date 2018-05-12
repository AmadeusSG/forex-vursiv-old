import requests
import boto3

class S3InstrumentStore:
    def __init__(self, access_key, secret_key, bucket):
        resource = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self.bucket = resource.Bucket(bucket)
     
    def put(self, key, data):
        return self.bucket.put_object(Key=key, Body=data.encode('utf-8')) 
                      
    def mput(self, datas):
        rows = []
        file_date = None
        for (info, data) in datas:
            if file_date is None:
                file_date = data.date
                   
            if same_day(file_date, data.date):
                rows.append(data.to_csv())
            else:
                key = self.file_key(info, file_date)
                csv = "\n".join(rows)   
                self.put(key, csv)
                rows = [csv]
                file_date = data.date
                
        if len(rows):
            key = self.file_key(info, file_date)
            csv = "\n".join(rows)
            self.put(key, csv)
        
    def file_key(self, info, date):
        return "{source}/{year}/{month:02d}/{day:02d}/{instrument}_{data_type}_{granularity}.csv"\
                    .format(source=info.source, year=date.year, month=date.month, day=date.day,\
                            instrument=info.instrument.lower(), data_type=info.type, granularity=info.granularity)