def voltToDb(configDict):
    import pandas as pd 
    path=configDict['file_path'] + '\\VOLTTEMP_22_07_2019.csv'
    df=pd.read_csv(path,skiprows=2,skipfooter=7)
    # columns_list=df.columns.tolist
    data=[]
    for ind in df.index:
        for col in df.columns.tolist()[1:]:
            tuple_value=(df['Timestamp'][ind],col,df[col][ind])
            data.append(tuple_value)
    print(data[-1])