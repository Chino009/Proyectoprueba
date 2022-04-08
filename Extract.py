import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, timedelta

class Extract: 
    
            
    def read_csv_to_df(self,bucket,key):
        csv_obj = bucket.Object(key=key).get().get('Body').read().decode('utf-8')
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=',')
        return df

    def return_objects(self,bucket,key,src_format,arg_date):
        arg_date_dt = datetime.strptime(arg_date, src_format).date() - timedelta(days=1)
        objects = [obj for obj in bucket.objects.all() if datetime.strptime(obj.key.split('/')[0], src_format).date() >= arg_date_dt]
        return objects

    def extract (self, objects, bucket):

        df_all = pd.concat([self.read_csv_to_df(bucket,obj.key) for obj in objects], ignore_index=True)
    
        return df_all
        
        