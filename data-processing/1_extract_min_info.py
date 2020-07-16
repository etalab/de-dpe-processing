import pandas as pd
import unidecode
import sys

if len(sys.argv) > 1:
    nb_dep = int(sys.argv[1])
else:
    nb_dep = 97

for i in range(nb_dep):
    dpt = i + 1
    if(dpt != 20):
        df = pd.read_csv('../data/dep/'+str(dpt)+'/td001_dpe.csv', dtype={'numero_dpe':str,'numero_rue':str,'type_voie':str,'nom_rue':str,'code_insee_commune_actualise':str}, keep_default_na=False, na_values='')

        values = {'numero_rue': '', 'type_voie':'','nom_rue':'','code_insee_commune_actualise':'','commune':''}
        df = df.fillna(value=values)
        
        df["exists"] = df.drop("numero_rue", 1).isin(df["numero_rue"])[['nom_rue']]
        df.loc[df['exists'] == False, "new_numero_rue"] = df['numero_rue']

        df["exists2"] = df.drop("type_voie", 1).isin(df["type_voie"])[['nom_rue']]
        df.loc[df['exists2'] == False, "new_type_voie"] = df['type_voie']

        df["exists3"] = df.drop("code_insee_commune_actualise", 1).isin(df["code_insee_commune_actualise"])[['nom_rue']]
        df.loc[df['exists3'] == False, "new_code_insee_commune_actualise"] = df['code_insee_commune_actualise']

        df['new_code_insee_commune_actualise'] = df['new_code_insee_commune_actualise'].apply(lambda x: x if len(str(x)) != 4 else '0'+x)

        df["exists4"] = df.drop("commune", 1).isin(df["commune"])[['nom_rue']]
        df.loc[df['exists4'] == False, "new_commune"] = df['commune']


        df['concat-adress'] = df['new_numero_rue'].astype(str) + " " + df['new_type_voie'].astype(str) + " " + df['nom_rue'].astype(str) + " " + df['new_code_insee_commune_actualise'].astype(str) + " " + df['new_commune'].astype(str)
    
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
        df['concat-adress'] = df['concat-adress'].replace(r'\\n',' ', regex=True) 
        df['concat-adress'] = df['concat-adress'].str.replace('\r', ' ')  
        df['concat-adress'] = df['concat-adress'].str.replace('\n', ' ') 
        df['concat-adress'] = df['concat-adress'].str.replace('NC ', ' ')
        df['concat-adress'] = df['concat-adress'].str.replace('_', ' ')
        df['concat-adress'] = df['concat-adress'].str.replace('NULL', ' ')
        df['concat-adress'] = df['concat-adress'].str.replace('Non communiqué', '')
        df['concat-adress'] = df['concat-adress'].str.replace('Habitation', '')
        df['concat-adress'] = df['concat-adress'].str.replace('Studio', '')
        df['concat-adress'] = df['concat-adress'].str.replace('appt', '')
        df['concat-adress'] = df['concat-adress'].str.replace('Lotist', '')
        df['concat-adress'] = df['concat-adress'].str.replace('n°', '')
        df['concat-adress'] = df['concat-adress'].str.replace('/', ' ')
        df['concat-adress'] = df['concat-adress'].apply(lambda x: unidecode.unidecode(x))


        df['concat-adress'] = df["concat-adress"].str.encode('utf-8', 'ignore').str.decode('utf-8')

        dfmini = df[['numero_dpe', 'concat-adress']]

        dfmini.to_csv('../data-processing/geocodage/1-mini/td001_dpe-'+str(dpt)+'.csv', index=False)
        print(str(dpt)+" done")