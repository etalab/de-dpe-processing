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



def agg_td006_td001(td006):

    td001_td006 = td006.sort_values('td001_dpe_id').drop_duplicates(subset=['td001_dpe_id'])

    td001_td006 = td001_td006[
        ['td001_dpe_id', 'tv014_type_prise_air', 'tv015_type', 'tv026_classe_inertie_id']].set_index('td001_dpe_id')

    td001_td006.columns = ['type_prise_air', 'type_ventilation', 'tv026_classe_inertie_id']

    return td001_td006
