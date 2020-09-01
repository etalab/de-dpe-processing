import pandas as pd
import os

csvfiles = os.listdir("../data/1-csv")

for i in range(len(csvfiles)):
    
    print("loading : "+csvfiles[i])
    
    df = pd.read_csv(open("../data/1-csv/"+csvfiles[i],'rU'), encoding='utf-8', engine='c')
    
    for j in range(97):
        numdep = j +1
        dfdpt = df[df['tv016_departement_id'] == numdep]
        dfdpt.to_csv('../data/2-dpt/'+csvfiles[i].split(".csv")[0]+'-'+str(numdep)+'.csv', index=False)
        print('../data/2-dpt/'+csvfiles[i].split(".csv")[0]+'-'+str(numdep)+'.csv done!')
        del dfdpt
    del df
