from io import StringIO, BytesIO

class Load:
    

    def write_df_to_s3(self,s3,trg_bucket,df_all,key):
        out_buffer = BytesIO()
        df_all.to_parquet(out_buffer, index=False)
        bucket_target = s3.Bucket(trg_bucket)
        bucket_target.put_object(Body=out_buffer.getvalue(), Key=key)
        return bucket_target

    def load(self,s3,trg_bucket,df_all,key):
        bucket_target = self.write_df_to_s3(s3,trg_bucket,df_all,key)
        listObj_key= []     
        for obj in bucket_target.objects.all():
            print(obj.key)
            listObj_key.append(obj.key)
            
        last_Key = listObj_key[-1]
        
        prq_obj = bucket_target.Object(key=last_Key).get().get('Body').read()
        data = BytesIO(prq_obj) 
    
        return data
    