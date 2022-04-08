import matplotlib.pyplot as plt
import pandas as pd

class Transform:
    

    def transform_report(self,df_all,arg_date,columns):
    
        df_all = df_all.loc[:, columns]
        df_all.dropna(inplace=True)
        
        # # Filtrar por horas (entre las 8:00 a 12:00)

        df_all= df_all.loc[(df_all["Time"] >= '08:00') & (df_all["Time"]<='12:00') , ["ISIN", "Date","Time","StartPrice","EndPrice"]]
        df_all
        df_all.dropna(inplace=True)
        df_all.shape
        df_all = df_all.round(decimals=2)
        df_all = df_all[df_all.Date >= arg_date]

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

        
        print("Esto es el df_all dentro de Transform", df_all)
        return df_all



