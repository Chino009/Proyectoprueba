import boto3
import pandas as pd
from io import StringIO, BytesIO #Se añadió BytesIO
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt


arg_date = '2022-03-02'


arg_date_dt = datetime.strptime(arg_date, '%Y-%m-%d').date() - timedelta(days=1)


arg_date_dt


s3 = boto3.resource('s3')
bucket = s3.Bucket('deutsche-boerse-xetra-pds')


objects = [obj for obj in bucket.objects.all() if datetime.strptime(obj.key.split('/')[0], '%Y-%m-%d').date() >= arg_date_dt]


objects


csv_obj_init = bucket.Object(key=objects[0].key).get().get('Body').read().decode('utf-8')
data = StringIO(csv_obj_init)
df_init = pd.read_csv(data, delimiter=',')


df_init.columns


df_all = pd.DataFrame(columns=df_init.columns)
for obj in objects:
    csv_obj = bucket.Object(key=obj.key).get().get('Body').read().decode('utf-8')
    data = StringIO(csv_obj)
    df = pd.read_csv(data, delimiter=',')
    df_all = df_all.append(df, ignore_index=True)


df_all


# # Filtrar por horas (entre las 8:00 a 12:00)


df_all= df_all.loc[(df_all["Time"] >= '08:00') & (df_all["Time"]<='12:00') , ["ISIN", "Date","Time","StartPrice","EndPrice"]]


df_all


df_all.dropna(inplace=True)


df_all.shape


# # Desviacion Estandar


df_all['STD'] = df_all[["StartPrice", "EndPrice"]].std(axis= 1)


df_all


# # Conversion de EndPrice (Euros) a pesos mexicanos


df_all['MXN'] = df_all['EndPrice'] * 22.83


df_all


# # Regresion lineal


df_all.plot(x='EndPrice', y='MXN', style='o')
plt.title('EndPrice - MXN')
plt.xlabel('EndPrice') 
plt.ylabel('MXN') 
plt.show()


X = df_all['EndPrice'].values.reshape(-1,1)
y = df_all['MXN'].values.reshape(-1,1)


from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)


from sklearn.linear_model import LinearRegression
lr = LinearRegression() 
lr.fit(X_train, y_train)


print(lr.intercept_)


print(lr.coef_)


y_pred = lr.predict(X_test)


df_prediccion = pd.DataFrame({'Actual': y_test.flatten(), 'Prediccion': y_pred.flatten()})


df_prediccion


plt.scatter(X_test, y_test,  color='blue')
plt.plot(X_test, y_pred, color='red', linewidth=2)
plt.show()


df_all = pd.concat([df_all, df_prediccion])


df_all = df_all.infer_objects()


df_all.dtypes


df_all


# ## Write to S3


key = 'xetra_daily_report_' + datetime.today().strftime("%Y%m%d_%H%M%S") + '.parquet'


out_buffer = BytesIO()
df_all.to_parquet(out_buffer, index=False)
bucket_target = s3.Bucket('xetra-bucket-p1p1')
bucket_target.put_object(Body=out_buffer.getvalue(), Key=key)


# ## Reading the uploaded file


for obj in bucket_target.objects.all():
    print(obj.key)


prq_obj = bucket_target.Object(key='xetra_daily_report_20220303_215943.parquet').get().get('Body').read()
data = BytesIO(prq_obj)
df_report = pd.read_parquet(data)


df_report