


td011_types={'td011_installation_chauffage_id': 'str',
 'td006_batiment_id': 'str',
 'tr003_type_installation_chauffage_id': 'category',
 'surface_chauffee': 'float',
 'nombre_appartements_echantillon': 'float',
 'surface_habitable_echantillon': 'float',
 'tv025_intermittence_id': 'category',
 'td001_dpe_id': 'str'}

def merge_td011_tr_tv(td011):
    from assets_orm import DPEMetaData
    meta = DPEMetaData()
    table = td011.copy()
    table = meta.merge_all_tr_table(table)
    table = meta.merge_all_tv_table(table)
    #table = table.astype(td007_types)
    table = table.rename(columns={'id': 'td007_paroi_opaque_id'})

    return table