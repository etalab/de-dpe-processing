
def merge_td001_dpe_id_envelope(td001, td006, td007, td008,td010):
    """
    preparation des tables enveloppe en fournissant le td001_dpe_id pour toutes les tables
    """

    td006 = td006.rename(columns={"id": "td006_batiment_id"})
    td007 = td007.rename(columns={"id": "td007_paroi_opaque_id"})
    td008 = td008.rename(columns={"id": "td008_baie_id"})
    td001 = td001.rename(columns={"id": "td001_dpe_id"})
    td007 = td007.merge(td006[['td006_batiment_id', 'td001_dpe_id']], on='td006_batiment_id', how='left')
    td008 = td008.merge(td007[['td007_paroi_opaque_id', 'td001_dpe_id']], on='td007_paroi_opaque_id', how='left')
    td010 = td010.merge(td006[['td006_batiment_id', 'td001_dpe_id']], on='td006_batiment_id', how='left')

    return td001,td006,td007,td008,td010


def merge_td001_dpe_id_system(td001, td006, td011, td012,td013,td014):
    """
    preparation des tables systèmes en fournissant le td001_dpe_id pour toutes les tables
    """
    td001 = td001.rename(columns={"id": "td001_dpe_id"})
    td006 = td006.rename(columns={"id": "td006_batiment_id"})
    td011 = td011.rename(columns={"id": "td011_installation_chauffage_id"})
    td012 = td012.rename(columns={"id": "td012_generateur_chauffage_id"})
    td013 = td013.rename(columns={"id": "td013_installation_ecs_id"})
    td014 = td014.rename(columns={"id": "td014_generateur_ecs_id"})

    td011 = td011.merge(td006[['td006_batiment_id', 'td001_dpe_id']], on='td006_batiment_id', how='left')
    td012 = td012.merge(td011[['td011_installation_chauffage_id', 'td001_dpe_id']], on='td011_installation_chauffage_id', how='left')

    td013 = td013.merge(td006[['td006_batiment_id', 'td001_dpe_id']], on='td006_batiment_id', how='left')
    td014 = td014.merge(td013[['td013_installation_ecs_id', 'td001_dpe_id']], on='td013_installation_ecs_id', how='left')


    return td001,td006,td011, td012,td013,td014


def merge_count_subtables(td001,td006,td007,td008):
    """
    obsolete

    """

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
