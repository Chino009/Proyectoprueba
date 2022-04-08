from Extract import Extract
from Load import Load
from Transform import Transform
from datetime import datetime, timedelta
import boto3
import pandas as pd


class Main:
    
        # Parameters/Configurations    
        arg_date = ''
        src_format = '%Y-%m-%d'
        src_bucket = 'deutsche-boerse-xetra-pds'
        trg_bucket = 'xetra-bucket-p1p1'
        columns = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice', 'EndPrice', 'TradedVolume']
        key = 'xetra_daily_report_' + datetime.today().strftime("%Y%m%d_%H%M%S") + '.parquet'
        
        # Init
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(src_bucket)
        
        # run application
        arg_date= input("Ingresa una fecha yyyy-mm-dd:") 
        objExtract = Extract()  
        objects= objExtract.return_objects(bucket,key,src_format,arg_date)
        
        df_all = objExtract.extract(objects,bucket)
       
        objTransform = Transform()
        
        df_all = objTransform.transform_report(df_all,arg_date,columns)
        objLoad = Load()

        data = objLoad.load(s3,trg_bucket,df_all,key)

        df_report = pd.read_parquet(data)
        print("Esto es el df report",df_report)



       