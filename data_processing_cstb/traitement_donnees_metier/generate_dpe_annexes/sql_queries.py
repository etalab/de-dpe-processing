import pandas as pd
import numpy as np
schema_name = "adedpe202006"
td001_cols = 'td001_dpe.id as td001_dpe_id,td001_dpe.annee_construction as annee_construction,td001_dpe.tv016_departement_id as tv016_departement_id'

def convert_id_column(table, col):
    table[col] = table[col].astype(dtype=pd.Int32Dtype()).astype(str).replace('<NA>', np.nan).astype('category')

def convert_all_tr_tv_ids(table):
    ids_cols = [col for col in table if col.endswith('id')]

    for col in ids_cols:
        print(col)
        convert_id_column(table, col)
    return table

def get_td006(dept, engine, schema_name=schema_name):
    query = f"""
        SELECT td006_batiment.*,{td001_cols}
        from {schema_name}.td006_batiment as  td006_batiment
        INNER JOIN {schema_name}.td001_dpe as td001_dpe
                    ON td001_dpe.id = td006_batiment.td001_dpe_id
        WHERE td001_dpe.tv016_departement_id = {dept}
        """
    table = pd.read_sql(query, engine)
    table = table.rename(columns={"id": "td006_batiment_id"})
    table = convert_all_tr_tv_ids(table)
    return table


def get_td007(dept, engine, schema_name=schema_name):
    query = f"""
        SELECT td007_paroi_opaque.*,td006_batiment_id,{td001_cols}
        from {schema_name}.td007_paroi_opaque as  td007_paroi_opaque
        INNER JOIN {schema_name}.td006_batiment as td006_batiment
                    ON td006_batiment.id = td007_paroi_opaque.td006_batiment_id
        INNER JOIN {schema_name}.td001_dpe as td001_dpe
                    ON td001_dpe.id = td006_batiment.td001_dpe_id
        WHERE td001_dpe.tv016_departement_id = {dept}
        """
    table = pd.read_sql(query, engine)
    table = table.rename(columns={"id": "td007_paroi_opaque_id"})
    table = convert_all_tr_tv_ids(table)

    return table

def get_td008(dept, engine, schema_name=schema_name):

    query = f"""
        SELECT td008_baie.*,td007_paroi_opaque_id,td006_batiment_id,{td001_cols}
        from {schema_name}.td008_baie as  td008_baie
        INNER JOIN {schema_name}.td007_paroi_opaque as td007_paroi_opaque
            ON td007_paroi_opaque.id = td008_baie.td007_paroi_opaque_id
        INNER JOIN {schema_name}.td006_batiment as td006_batiment
                    ON td006_batiment.id = td007_paroi_opaque.td006_batiment_id
        INNER JOIN {schema_name}.td001_dpe as td001_dpe
                    ON td001_dpe.id = td006_batiment.td001_dpe_id
        WHERE td001_dpe.tv016_departement_id = {dept}
        """
    table = pd.read_sql(query, engine)
    if 'td008_baie_id' in table:
        del table['td008_baie_id']
    table = table.rename(columns={"id": "td008_baie_id"})
    table = convert_all_tr_tv_ids(table)

    return table

def get_td010(dept, engine, schema_name=schema_name):
    query = f"""
        SELECT td010_pont_thermique.*,td006_batiment_id,{td001_cols}
        from {schema_name}.td010_pont_thermique as  td010_pont_thermique
        INNER JOIN {schema_name}.td006_batiment as td006_batiment
                    ON td006_batiment.id = td010_pont_thermique.td006_batiment_id
        INNER JOIN {schema_name}.td001_dpe as td001_dpe
                    ON td001_dpe.id = td006_batiment.td001_dpe_id
        WHERE td001_dpe.tv016_departement_id = {dept}
        """
    table = pd.read_sql(query, engine)
    table = table.rename(columns={"id": "td010_pont_thermique_id"})
    table = convert_all_tr_tv_ids(table)

    return table