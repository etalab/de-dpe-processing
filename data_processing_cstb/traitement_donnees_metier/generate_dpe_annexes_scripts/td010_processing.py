import pandas as pd
import numpy as np
from .utils import agg_pond_top_freq, agg_pond_avg
from .trtvtables import DPETrTvTables

td010_types = {'longueur': 'float'}


def merge_td010_tr_tv(td010):
    meta = DPETrTvTables()
    table = td010.copy()
    table = meta.merge_all_tr_tables(table)

    table = meta.merge_all_tv_tables(table)

    table = table.loc[:, ~table.columns.duplicated()]

    return table


def postprocessing_td010(td010):
    td010 = td010.astype(td010_types)
    type_liaison_to_varname = {'Mensuiserie / Plancher haut': 'menui_ph',
                               'Menuiserie / Mur': 'menui_mur',
                               'Plancher bas / Mur': 'pb_mur',
                               'Plancher haut lourd / Mur': 'ph_mur',
                               'Plancher intermédiaire lourd / Mur': 'pi_mur',
                               'Refend / Mur': 'refend_mur', }

    td010['type_liaison'] = td010.tv013_type_liaison.replace(type_liaison_to_varname)

    return td010


def agg_td010_td001(td010):
    long = td010.pivot_table(index='td001_dpe_id', columns='type_liaison', values='longueur', aggfunc='sum')
    long.columns = [f'longueur_{col}' for col in long]

    type_isol_mur = agg_pond_top_freq(td010, 'tv013_isolation_mur', 'longueur',
                                      'td001_dpe_id').to_frame('position_isolation_mur')
    td010_pb = td010.loc[td010.type_liaison == 'pb_mur']
    type_isol_plancher = agg_pond_top_freq(td010_pb, 'tv013_plancher_bas', 'longueur',
                                           'td001_dpe_id').to_frame('position_isolation_plancher')

    td010_ph = td010.loc[td010.type_liaison == 'ph_mur']
    type_isol_plafond = agg_pond_top_freq(td010_ph, 'tv013_plancher_bas', 'longueur',
                                          'td001_dpe_id').to_frame('position_isolation_plancher')

    td010_pt_agg = pd.concat([type_isol_mur, type_isol_plancher, type_isol_plafond, long], axis=1)
    td010_pt_agg.index.name = 'td001_dpe_id'

    return td010_pt_agg
