import pandas as pd
import numpy as np
from sqlalchemy import inspect
from generate_dpe_annexes.utils import round_float_cols
from generate_dpe_annexes.sql_config import engine, sql_config, td001_cols


def convert_id_column(table, col):
    table[col] = table[col].astype(dtype=pd.Int32Dtype()).astype(str).replace('<NA>', np.nan).astype('category')


def convert_all_tr_tv_ids(table):
    ids_cols = [col for col in table if (col.endswith('id') and not col.startswith('td'))]

    for col in ids_cols:
        convert_id_column(table, col)
    return table


def convert_td_ids(table):
    ids_cols = [col for col in table if (col.endswith('id') and col.startswith('td'))]
    for col in ids_cols:
        table[col] = table[col].astype(str)
    return table


def convert_all_ids(table):
    table = convert_all_tr_tv_ids(table)
    table = convert_td_ids(table)
    return table


def get_raw_departements():
    schema_name = sql_config['schemas']['dpe_raw_schema_name']
    query = f"""
    SELECT DISTINCT(tv016_departement_id) FROM {schema_name}.td001_dpe
    """

    df = pd.read_sql(query, engine)

    return [str(el) for el in df.tv016_departement_id.tolist()]


def get_annexe_departements(annexe_table_name):
    schema_name = sql_config['schemas']['dpe_out_schema_name']
    inspector = inspect(engine)
    if annexe_table_name in inspector.get_table_names(schema_name):
        query = f"""
        SELECT DISTINCT(tv016_departement_id) FROM {schema_name}.{annexe_table_name}
        """
        df = pd.read_sql(query, engine)

        return df.tv016_departement_id.tolist()
    else:
        return []


def get_td001(dept):
    schema_name = sql_config['schemas']['dpe_raw_schema_name']
    query = f"""
        SELECT td001_dpe.*
        from {schema_name}.td001_dpe as  td001_dpe
        WHERE td001_dpe.tv016_departement_id = {dept}
        """
    table = pd.read_sql(query, engine)
    table = table.rename(columns={"id": "td001_dpe_id"})
    table = table.loc[:, table.columns.duplicated() == False]
    table = convert_all_ids(table)
    return table


def get_td006(dept):
    schema_name = sql_config['schemas']['dpe_raw_schema_name']
    query = f"""
        SELECT td006_batiment.*,{td001_cols}
        from {schema_name}.td006_batiment as  td006_batiment
        INNER JOIN {schema_name}.td001_dpe as td001_dpe
                    ON td001_dpe.id = td006_batiment.td001_dpe_id
        WHERE td001_dpe.tv016_departement_id = {dept}
        """
    table = pd.read_sql(query, engine)
    table = table.rename(columns={"id": "td006_batiment_id"})
    table = table.loc[:, table.columns.duplicated() == False]
    table = convert_all_ids(table)
    return table


def get_td005(dept):
    schema_name = sql_config['schemas']['dpe_raw_schema_name']
    query = f"""
        SELECT td005_fiche_technique.*,{td001_cols}
        from {schema_name}.td005_fiche_technique as  td005_fiche_technique
        INNER JOIN {schema_name}.td001_dpe as td001_dpe
                    ON td001_dpe.id = td005_fiche_technique.td001_dpe_id
        WHERE td001_dpe.tv016_departement_id = {dept}
        """
    table = pd.read_sql(query, engine)
    table = table.rename(columns={"id": "td005_fiche_technique_id"})
    table = table.loc[:, table.columns.duplicated() == False]
    table = convert_all_ids(table)
    return table


def get_td003(dept):
    schema_name = sql_config['schemas']['dpe_raw_schema_name']
    query = f"""
        SELECT td003_descriptif.*,{td001_cols}
        from {schema_name}.td003_descriptif as  td003_descriptif
        INNER JOIN {schema_name}.td001_dpe as td001_dpe
                    ON td001_dpe.id = td003_descriptif.td001_dpe_id
        WHERE td001_dpe.tv016_departement_id = {dept}
        """
    table = pd.read_sql(query, engine)
    table = table.rename(columns={"id": "td003_descriptif_id"})
    table = table.loc[:, table.columns.duplicated() == False]
    table = convert_all_ids(table)
    return table


def get_td007(dept):
    schema_name = sql_config['schemas']['dpe_raw_schema_name']
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
    table = table.loc[:, table.columns.duplicated() == False]
    table = convert_all_ids(table)

    return table


def get_td008(dept):
    schema_name = sql_config['schemas']['dpe_raw_schema_name']
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
    table = table.loc[:, table.columns.duplicated() == False]
    table = convert_all_ids(table)

    return table


def get_td010(dept):
    schema_name = sql_config['schemas']['dpe_raw_schema_name']

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
    table = table.loc[:, table.columns.duplicated() == False]
    table = convert_all_ids(table)

    return table


def dump_sql(table, table_name, dept):
    schema_name = sql_config['schemas']['dpe_out_schema_name']
    inspector = inspect(engine)
    if table_name in inspector.get_table_names(schema_name):
        print('delete old data')
        delete_query = f"""
        DELETE
        FROM {schema_name}.{table_name}
        WHERE tv016_departement_id = '{dept}'
        """
        with engine.connect() as con:
            resp = con.execute(delete_query)
    round_float_cols(table).to_sql(table_name, con=engine, schema=schema_name, if_exists="append")
