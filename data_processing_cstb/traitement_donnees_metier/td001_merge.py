import pandas as pd

def merge_count_subtables(td001,td006,td007,td008):

    # ENVELOPPE
    td006 = td006.rename(columns={"id": "td006_batiment_id"})
    td007 = td007.rename(columns={"id": "td007_paroi_opaque_id"})
    td001 = td001.rename(columns={"id": "td001_dpe_id"})

    td008_count = td008.groupby('td007_paroi_opaque_id').id.count().to_frame('td008_count')
    td007 = td007.merge(td008_count.reset_index(), on='td007_paroi_opaque_id', how='left')

    td007_count = td007.groupby('td006_batiment_id').td007_paroi_opaque_id.count().to_frame('td007_count')

    td008_count_td007 = td007.groupby('td006_batiment_id').td008_count.sum().to_frame('td008_count')

    td006 = td006.merge(td007_count, on='td006_batiment_id', how='left')
    td006 = td006.merge(td008_count_td007, on='td006_batiment_id', how='left')

    td001_agg = td006.groupby('td001_dpe_id')[['td008_count', 'td007_count', 'td006_batiment_id']].agg(
        {'td008_count': 'sum',
         'td007_count': "sum",
         "td006_batiment_id": 'count'}).rename(columns={'td006_batiment_id': 'td006_count'})

    td001 = td001.merge(td001_agg.reset_index(), on='td001_dpe_id', how='left')

    td001[['is_td008', 'is_td007', 'is_td006']] = td001[['td008_count', 'td007_count', 'td006_count']] > 0

def merge_td001_dpe_id_envelope(td001, td006, td007, td008):
    td006 = td006.rename(columns={"id": "td006_batiment_id"})
    td007 = td007.rename(columns={"id": "td007_paroi_opaque_id"})
    td001 = td001.rename(columns={"id": "td001_dpe_id"})
    td007 = td007.merge(td006[['td006_batiment_id', 'td001_dpe_id']], on='td006_batiment_id', how='left')
    td008 = td008.merge(td007[['td007_paroi_opaque_id', 'td001_dpe_id']], on='td007_paroi_opaque_id', how='left')
    return td007,td008