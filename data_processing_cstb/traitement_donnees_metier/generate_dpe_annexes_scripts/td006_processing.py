import pandas as pd
import numpy as np
from .utils import agg_pond_top_freq, agg_pond_avg
from .trtvtables import DPETrTvTables

td006_types = {'longueur': 'float'}


def merge_td006_tr_tv(td006):
    meta = DPETrTvTables()
    table = td006.copy()
    table = meta.merge_all_tr_tables(table)

    table = meta.merge_all_tv_tables(table)

    table = table.loc[:, ~table.columns.duplicated()]

    return table


def processing_td006(td006):

    td006['type_ventilation'] = td006.tv015_type.astype(str).fillna('NA').value_counts()

    inertie_dict = {'TV026_003': "Lourde",
                    'TV026_008': "Légère",
                    'TV026_007': "Moyenne",
                    'TV026_001': "Très Lourde",
                    'TV026_002': "Lourde",
                    'TV026_004': "Lourde",
                    'TV026_005': "Moyenne",
                    'TV026_006': "Moyenne"}

    td006['inertie'] = td006.tv026_code.replace(inertie_dict)

    td006['type_prise_air'] = td006.tv014_type_prise_air

    return td006

def agg_td006_td001(td006):
    td006 = processing_td006(td006)
    td001_td006 = td006.sort_values('td001_dpe_id').drop_duplicates(subset=['td001_dpe_id'])

    td001_td006 = td001_td006[
        ['td001_dpe_id', 'type_prise_air', 'type_ventilation', 'inertie']].set_index('td001_dpe_id')

    return td001_td006
