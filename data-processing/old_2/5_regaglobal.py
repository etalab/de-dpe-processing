import pandas as pd
import os
import sys
import csv

df = pd.read_csv("../data/dep/1/td001_dpe-geocoded.csv",dtype=str)
print("Dep : 1")

for i in range(96):
    nb = i +2
    if(nb != 20):
        df2 = pd.read_csv("../data/dep/"+str(nb)+"/td001_dpe-geocoded.csv",dtype=str)
        df = df.append(df2, ignore_index=True)
        print("Dep : "+str(nb))

df.to_csv("../data/csv/td001_dpe-geocoded.csv",index=False,encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
