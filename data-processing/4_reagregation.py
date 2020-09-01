import pandas as pd
import os
import sys
import csv

if len(sys.argv) > 1:
    nb_dep = int(sys.argv[1])
else:
    nb_dep = 97

for i in range(nb_dep):
    dpt = 1 + i
    if(dpt != 20):
        csvfiles = os.listdir("../data-processing/geocodage/3-mini-split-geocoded/"+str(dpt)+"/")
        print("Init for dpt : "+str(dpt))
        df = pd.read_csv("../data-processing/geocodage/3-mini-split-geocoded/"+str(dpt)+"/"+csvfiles[0])
        for j in range(len(csvfiles)-1):
            nbfile = j + 1
            df2 = pd.read_csv("../data-processing/geocodage/3-mini-split-geocoded/"+str(dpt)+"/"+csvfiles[nbfile])
            df = df.append(df2)
            #print("append "+str(j)+" ok!")
        df.to_csv('../data-processing/geocodage/4-mini-geocoded/td001_dpe-'+str(dpt)+'-geocoded.csv', index=False)
        print("End for dpt : "+str(dpt))

for i in range(nb_dep):
    nb = i +1
    if(nb != 20):
        print(nb)
        df = pd.read_csv("../data/dep/"+str(nb)+"/td001_dpe.csv",dtype=str)
        df2 = pd.read_csv("../data-processing/geocodage/4-mini-geocoded/td001_dpe-"+str(nb)+"-geocoded.csv",dtype=str)
        df3 = pd.merge(df, df2, on='numero_dpe', how='left')
        df3.drop(columns=['concat-adress'])
        df3.to_csv("../data/dep/"+str(nb)+"/td001_dpe-geocoded.csv",index=False,encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
