import pandas as pd
import os

csvfiles = os.listdir("../data/3-dpt-agg/")

for i in range(len(csvfiles)):
    df = pd.read_csv("../data/3-dpt-agg/"+csvfiles[i])
    print("../data/3-dpt-agg/"+csvfiles[i])
    dflight = df.drop(columns=['nom_locataire', 'escalier', 'etage','porte','nom_gestionnaire','adresse_gestionnaire','nom_proprietaire','prenom_proprietaire','nom_proprietaire_installations_communes','nom_centre_commercial'])
    dflight.to_csv("../data/4-dpt-agg-ano/"+csvfiles[i])

