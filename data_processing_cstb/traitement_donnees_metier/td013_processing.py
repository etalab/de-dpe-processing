import pandas as pd
import numpy as np
from assets_orm import DPEMetaData

td013_types={'id': 'str',
 'td006_batiment_id': 'str',
 'tr005_type_installation_ecs_id': 'category',
 'nombre_appartements_echantillon': 'float',
 'surface_habitable_echantillon': 'float',
 'becs': 'float',
 'tv039_formule_becs_id': 'category',
 'surface_alimentee': 'float'}

def merge_td013_tr_tv(td013):
    meta = DPEMetaData()
    table = td013.copy()
    table = meta.merge_all_tr_table(table)
    table = meta.merge_all_tv_table(table)
    table = table.astype(td013_types)
    table = table.rename(columns={'id': 'td013_installation_ecs_id'})

    return table