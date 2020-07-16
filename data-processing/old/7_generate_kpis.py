#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import geopandas as gpd
pd.options.display.max_columns = 100
pd.options.display.max_rows = 1000


print("Load external files")
#geojson département
departements_geo = gpd.read_file('../external/departements-version-simplifiee.geojson')
#coordonnées préfectures départements

dfpref = pd.read_csv("../external/prefectures.csv", sep=";")
dfpref = dfpref.drop(columns=['Nom', 'Commune', 'DeptNom'])
dfpref = dfpref.rename(columns={"DeptNum": "code_departement"})
dfpref = dfpref[(dfpref['code_departement'] != '2A') & (dfpref['code_departement'] != '2B')]
dfpref['code_departement'] = dfpref['code_departement'].apply(lambda x: str(x) if int(x) > 9 else '0'+str(x))

#Liste départements
dpt = pd.read_csv("../external/departements.csv")
dpt = dpt.rename(columns={"departmentCode": "code_departement", "departmentName": "nom_departement"})


for d in range(97):
    dep = 1+d 
    if(dep != 20):
        print("Dep "+str(dep))
        df = pd.read_csv("../data/5-geocodage/5-geocoded/td001_dpe-"+str(dep)+"-geocoded.csv")
        df = df.dropna(how='any', subset=['tv016_departement_id'])
        df.tv016_departement_id = df.tv016_departement_id.astype(int)
        df['code_departement'] = df['tv016_departement_id'].apply(lambda x: str(x) if x > 9 else '0'+str(x))

        result = pd.merge(df,dpt,on="code_departement")

        print("Calcul KPIs")
        result_eco_A = result[result['classe_consommation_energie'] == 'A']
        result_eco_B = result[result['classe_consommation_energie'] == 'B']
        result_eco_C = result[result['classe_consommation_energie'] == 'C']
        result_eco_D = result[result['classe_consommation_energie'] == 'D']
        result_eco_E = result[result['classe_consommation_energie'] == 'E']
        result_eco_F = result[result['classe_consommation_energie'] == 'F']
        result_eco_G = result[result['classe_consommation_energie'] == 'G']
        result_eco_H = result[result['classe_consommation_energie'] == 'H']
        result_eco_I = result[result['classe_consommation_energie'] == 'I']
        result_eco_N = result[result['classe_consommation_energie'] == 'N']

        result_ges_A = result[result['classe_estimation_ges'] == 'A']
        result_ges_B = result[result['classe_estimation_ges'] == 'B']
        result_ges_C = result[result['classe_estimation_ges'] == 'C']
        result_ges_D = result[result['classe_estimation_ges'] == 'D']
        result_ges_E = result[result['classe_estimation_ges'] == 'E']
        result_ges_F = result[result['classe_estimation_ges'] == 'F']
        result_ges_G = result[result['classe_estimation_ges'] == 'G']
        result_ges_H = result[result['classe_estimation_ges'] == 'H']
        result_ges_I = result[result['classe_estimation_ges'] == 'I']
        result_ges_N = result[result['classe_estimation_ges'] == 'N']

        stat_dpt_glob = pd.DataFrame(result['code_departement'].value_counts().reset_index().values, columns=["code_departement", "Nb"])

        stat_dpt_eco_A = pd.DataFrame(result_eco_A['code_departement'].value_counts().reset_index().values, columns=["code_departement", "Eco A"])
        stat_dpt_eco_B = pd.DataFrame(result_eco_B['code_departement'].value_counts().reset_index().values, columns=["code_departement", "Eco B"])
        stat_dpt_eco_C = pd.DataFrame(result_eco_C['code_departement'].value_counts().reset_index().values, columns=["code_departement", "Eco C"])
        stat_dpt_eco_D = pd.DataFrame(result_eco_D['code_departement'].value_counts().reset_index().values, columns=["code_departement", "Eco D"])
        stat_dpt_eco_E = pd.DataFrame(result_eco_E['code_departement'].value_counts().reset_index().values, columns=["code_departement", "Eco E"])
        stat_dpt_eco_F = pd.DataFrame(result_eco_F['code_departement'].value_counts().reset_index().values, columns=["code_departement", "Eco F"])
        stat_dpt_eco_G = pd.DataFrame(result_eco_G['code_departement'].value_counts().reset_index().values, columns=["code_departement", "Eco G"])
        stat_dpt_eco_H = pd.DataFrame(result_eco_H['code_departement'].value_counts().reset_index().values, columns=["code_departement", "Eco H"])
        stat_dpt_eco_I = pd.DataFrame(result_eco_I['code_departement'].value_counts().reset_index().values, columns=["code_departement", "Eco I"])
        stat_dpt_eco_N = pd.DataFrame(result_eco_N['code_departement'].value_counts().reset_index().values, columns=["code_departement", "Eco N"])

        stat_dpt_ges_A = pd.DataFrame(result_ges_A['code_departement'].value_counts().reset_index().values, columns=["code_departement", "GES A"])
        stat_dpt_ges_B = pd.DataFrame(result_ges_B['code_departement'].value_counts().reset_index().values, columns=["code_departement", "GES B"])
        stat_dpt_ges_C = pd.DataFrame(result_ges_C['code_departement'].value_counts().reset_index().values, columns=["code_departement", "GES C"])
        stat_dpt_ges_D = pd.DataFrame(result_ges_D['code_departement'].value_counts().reset_index().values, columns=["code_departement", "GES D"])
        stat_dpt_ges_E = pd.DataFrame(result_ges_E['code_departement'].value_counts().reset_index().values, columns=["code_departement", "GES E"])
        stat_dpt_ges_F = pd.DataFrame(result_ges_F['code_departement'].value_counts().reset_index().values, columns=["code_departement", "GES F"])
        stat_dpt_ges_G = pd.DataFrame(result_ges_G['code_departement'].value_counts().reset_index().values, columns=["code_departement", "GES G"])
        stat_dpt_ges_H = pd.DataFrame(result_ges_H['code_departement'].value_counts().reset_index().values, columns=["code_departement", "GES H"])
        stat_dpt_ges_I = pd.DataFrame(result_ges_I['code_departement'].value_counts().reset_index().values, columns=["code_departement", "GES I"])
        stat_dpt_ges_N = pd.DataFrame(result_ges_N['code_departement'].value_counts().reset_index().values, columns=["code_departement", "GES N"])

        dfkpi = pd.merge(stat_dpt_glob,stat_dpt_eco_A,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_eco_B,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_eco_C,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_eco_D,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_eco_E,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_eco_F,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_eco_G,on="code_departement")
        #dfkpi = pd.merge(dfkpi,stat_dpt_eco_H,on="code_departement")
        #dfkpi = pd.merge(dfkpi,stat_dpt_eco_I,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_eco_N,on="code_departement")

        dfkpi = pd.merge(dfkpi,stat_dpt_ges_A,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_ges_B,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_ges_C,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_ges_D,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_ges_E,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_ges_F,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_ges_G,on="code_departement")
        #dfkpi = pd.merge(dfkpi,stat_dpt_ges_H,on="code_departement")
        #dfkpi = pd.merge(dfkpi,stat_dpt_ges_I,on="code_departement")
        dfkpi = pd.merge(dfkpi,stat_dpt_ges_N,on="code_departement")

        dfkpi['Nb'] = dfkpi['Nb'].astype(int)
        dfkpi['Eco A'] = dfkpi['Eco A'].astype(int)
        dfkpi['Eco B'] = dfkpi['Eco B'].astype(int)
        dfkpi['Eco C'] = dfkpi['Eco C'].astype(int)
        dfkpi['Eco D'] = dfkpi['Eco D'].astype(int)
        dfkpi['Eco E'] = dfkpi['Eco E'].astype(int)
        dfkpi['Eco F'] = dfkpi['Eco F'].astype(int)
        dfkpi['Eco G'] = dfkpi['Eco G'].astype(int)
        dfkpi['Eco N'] = dfkpi['Eco N'].astype(int)

        dfkpi['GES A'] = dfkpi['GES A'].astype(int)
        dfkpi['GES B'] = dfkpi['GES B'].astype(int)
        dfkpi['GES C'] = dfkpi['GES C'].astype(int)
        dfkpi['GES D'] = dfkpi['GES D'].astype(int)
        dfkpi['GES E'] = dfkpi['GES E'].astype(int)
        dfkpi['GES F'] = dfkpi['GES F'].astype(int)
        dfkpi['GES G'] = dfkpi['GES G'].astype(int)
        dfkpi['GES N'] = dfkpi['GES N'].astype(int)

        dfkpi2 = dfkpi

        sum_column = (dfkpi2["Eco A"] / dfkpi2["Nb"])*100
        dfkpi2["stat_eco_a"] = sum_column
        sum_column = (dfkpi2["Eco B"] / dfkpi2["Nb"])*100
        dfkpi2["stat_eco_b"] = sum_column
        sum_column = (dfkpi2["Eco C"] / dfkpi2["Nb"])*100
        dfkpi2["stat_eco_c"] = sum_column
        sum_column = (dfkpi2["Eco D"] / dfkpi2["Nb"])*100
        dfkpi2["stat_eco_d"] = sum_column
        sum_column = (dfkpi2["Eco E"] / dfkpi2["Nb"])*100
        dfkpi2["stat_eco_e"] = sum_column
        sum_column = (dfkpi2["Eco F"] / dfkpi2["Nb"])*100
        dfkpi2["stat_eco_f"] = sum_column
        sum_column = (dfkpi2["Eco G"] / dfkpi2["Nb"])*100
        dfkpi2["stat_eco_g"] = sum_column
        #sum_column = (dfkpi2["Eco H"] / dfkpi2["Nb"])*100
        #dfkpi2["stat_eco_h"] = sum_column
        #sum_column = (dfkpi2["Eco I"] / dfkpi2["Nb"])*100
        #dfkpi2["stat_eco_i"] = sum_column
        sum_column = (dfkpi2["Eco N"] / dfkpi2["Nb"])*100
        dfkpi2["stat_eco_n"] = sum_column


        sum_column = (dfkpi2["GES A"] / dfkpi2["Nb"])*100
        dfkpi2["stat_ges_a"] = sum_column
        sum_column = (dfkpi2["GES B"] / dfkpi2["Nb"])*100
        dfkpi2["stat_ges_b"] = sum_column
        sum_column = (dfkpi2["GES C"] / dfkpi2["Nb"])*100
        dfkpi2["stat_ges_c"] = sum_column
        sum_column = (dfkpi2["GES D"] / dfkpi2["Nb"])*100
        dfkpi2["stat_ges_d"] = sum_column
        sum_column = (dfkpi2["GES E"] / dfkpi2["Nb"])*100
        dfkpi2["stat_ges_e"] = sum_column
        sum_column = (dfkpi2["GES F"] / dfkpi2["Nb"])*100
        dfkpi2["stat_ges_f"] = sum_column
        sum_column = (dfkpi2["GES G"] / dfkpi2["Nb"])*100
        dfkpi2["stat_ges_g"] = sum_column
        #sum_column = (dfkpi2["GES H"] / dfkpi2["Nb"])*100
        #dfkpi2["stat_ges_h"] = sum_column
        #sum_column = (dfkpi2["GES I"] / dfkpi2["Nb"])*100
        #dfkpi2["stat_ges_i"] = sum_column
        sum_column = (dfkpi2["GES N"] / dfkpi2["Nb"])*100
        dfkpi2["stat_ges_n"] = sum_column

        dfkpi3 = dfkpi2

        dfkpi3['percent_eco_abc'] = dfkpi3['stat_eco_a'] + dfkpi3['stat_eco_b'] + dfkpi3['stat_eco_c']
        dfkpi3['percent_ges_abc'] = dfkpi3['stat_ges_a'] + dfkpi3['stat_ges_b'] + dfkpi3['stat_ges_c']

        dfkpi3

        dfkpi3 = pd.merge(dfkpi3,dpt,on="code_departement")
        dfkpi4 = pd.merge(dfkpi3,dfpref,on="code_departement")
        if(dep == 1):
            dfglobal = dfkpi4
        else:
            dfglobal = dfglobal.append(dfkpi4)
dfglobal.to_csv("../data/6-kpis/td001_dpe_kpi.csv", index=False)


