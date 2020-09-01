for i in range(77):
    nb = i +21
    print(nb)
    df = pd.read_csv("../data/4-dpt-agg-ano/td001_dpe-"+str(nb)+".csv")
    df2 = pd.read_csv("../data/5-geocodage/4-mini-geocoded/td001_dpe-"+str(nb)+"-geocoded.csv")
    df3 = pd.merge(df, df2, left_on='id', right_on='id', how='left').drop('id', axis=1)
    df3.to_csv("../data/5-geocodage/5-geocoded/td001_dpe-"+str(nb)+"-geocoded.csv", index=False)