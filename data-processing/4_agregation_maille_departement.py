import pandas as pd
for i in range(97):
    dpt = i + 1
    print("Init for dpt : "+str(dpt))
    df = pd.read_csv("../data/2-dpt/td001_dpe-1-"+str(dpt)+".csv")
    for j in range(9):
        nbfile = j + 1
        df2 = pd.read_csv("../data/2-dpt/td001_dpe-"+str(nbfile)+"-"+str(dpt)+".csv")
        df = df.append(df2)
        print("append "+str(j)+" ok!")
    df.to_csv('../data/3-dpt-agg/td001_dpe-'+str(dpt)+'.csv', index=False)
    print("End for dpt : "+str(dpt))