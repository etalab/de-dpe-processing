import pandas as pd

for i in range(97):
    dpt = i + 1
    print(dpt)
    df = pd.read_csv('../data/4-dpt-agg-ano/td001_dpe-'+str(dpt)+'.csv', keep_default_na=False, na_values='')

    values = {'numero_rue': '', 'type_voie':'','nom_rue':'','code_insee_commune_actualise':'','commune':''}
    df = df.fillna(value=values)
    
    df['concat-adress'] = df['numero_rue'].astype(str) + " " + df['type_voie'].astype(str) + " " + df['nom_rue'].astype(str) + " " + df['code_insee_commune_actualise'].astype(str) + " " + df['commune'].astype(str)
   
    df['concat-adress'] = df['concat-adress'].astype(str)
    
    df['concat-adress'] = df['concat-adress'].str.replace('APPARTEMENT ', '')
    df['concat-adress'] = df['concat-adress'].str.replace('Appartement ', '')
    df['concat-adress'] = df['concat-adress'].str.replace('Apt ', '')
    df['concat-adress'] = df['concat-adress'].str.replace('Apt.', '')
    df['concat-adress'] = df['concat-adress'].str.replace('apt ', '')
    df['concat-adress'] = df['concat-adress'].str.replace('apt.', '')
    df['concat-adress'] = df['concat-adress'].str.replace(',', '')
    df['concat-adress'] = df['concat-adress'].str.replace('non communiquée', '')
    df['concat-adress'] = df['concat-adress'].str.replace('LOTISSEMENT ', '')
    df['concat-adress'] = df['concat-adress'].str.replace('Lotissement ', '')
    df['concat-adress'] = df['concat-adress'].str.replace('lotissement ', '')
    df['concat-adress'] = df['concat-adress'].str.replace('LOT ', '')
    df['concat-adress'] = df['concat-adress'].str.replace('Lot ', '')
    df['concat-adress'] = df['concat-adress'].str.replace('lot ', '')
    df['concat-adress'] = df['concat-adress'].str.replace('\'', ' ')
    df['concat-adress'] = df['concat-adress'].str.replace('\"', ' ')
    df['concat-adress'] = df['concat-adress'].str.replace('\n', ' ')
    df['concat-adress'] = df['concat-adress'].str.replace('NC ', ' ')
    df['concat-adress'] = df['concat-adress'].str.replace('_', ' ')
    df['concat-adress'] = df["concat-adress"].str.encode('utf-8', 'ignore').str.decode('utf-8')

    dfmini = df[['id', 'concat-adress']]

    dfmini.to_csv('../data/5-geocodage/1-mini/td001_dpe-'+str(dpt)+'.csv', index=False)