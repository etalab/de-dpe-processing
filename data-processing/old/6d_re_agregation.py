import pandas as pd
import os
for i in range(77):
    dpt = 21 + i
    csvfiles = os.listdir("../data/5-geocodage/3-mini-split-geocoded/"+str(dpt)+"/")
    print("Init for dpt : "+str(dpt))
    df = pd.read_csv("../data/5-geocodage/3-mini-split-geocoded/"+str(dpt)+"/"+csvfiles[0])
    for j in range(len(csvfiles)-1):
        nbfile = j + 1
        df2 = pd.read_csv("../data/5-geocodage/3-mini-split-geocoded/"+str(dpt)+"/"+csvfiles[nbfile])
        df = df.append(df2)
        print("append "+str(j)+" ok!")
    df.to_csv('../data/5-geocodage/4-mini-geocoded/td001_dpe-'+str(dpt)+'-geocoded.csv', index=False)
    print("End for dpt : "+str(dpt))
