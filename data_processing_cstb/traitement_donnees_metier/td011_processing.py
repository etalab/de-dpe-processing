




def merge_td011_tr_tv(td011):
    from assets_orm import DPEMetaData
    meta = DPEMetaData()
    table = td011.copy()
    table = meta.merge_all_tr_table(table)
    table = meta.merge_all_tv_table(table)
    #table = table.astype(td007_types)
    table = table.rename(columns={'id': 'td007_paroi_opaque_id'})

    return table