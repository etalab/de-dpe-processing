from .trtvtables import DPETrTvTables
from .conversion_normalisation import energy_cols, energie_normalise_ordered,ener_conv_dict

def merge_td002_td016_trtrv(td002,td016):

    trtv = DPETrTvTables()
    td002 = trtv.merge_all_trtv_tables(td002)
    td016 = trtv.merge_all_trtv_tables(td016)
    return td002,td016

def extract_type_energie_from_td002_td016(td001, td002, td016):

    def extract_type_energie_ch_ecs(table):
        table_ch = table.loc[table.tr006_type_usage_id.isin(tr006_ch_usage.keys())].copy()

        table_ch['energie_concat'] = ''
        for col in energy_cols:
            if col in table_ch:
                table_ch['energie_concat'] += table_ch[col].astype('string').fillna('indetermine').replace(
                    ener_conv_dict[col]).astype(str).fillna('').str.lower()
        for energie in reversed(energie_normalise_ordered):
            is_ener = table_ch.energie_concat.str.contains(r'{}'.format(energie))
            table_ch.loc[is_ener, 'type_energie'] = energie

        table_ecs = table.loc[table.tr006_type_usage_id.isin(tr006_ecs_usage.keys())].copy()

        table_ecs['energie_concat'] = ''
        for col in energy_cols:
            if col in table_ecs:
                table_ecs['energie_concat'] += table_ecs[col].astype('string').fillna('indetermine').replace(
                    ener_conv_dict[col]).astype(str).fillna('').str.lower()
        for energie in reversed(energie_normalise_ordered):
            is_ener = table_ecs.energie_concat.str.contains(r'{}'.format(energie))
            table_ecs.loc[is_ener, 'type_energie'] = energie
        return table_ch, table_ecs



    tr006_ch_usage = {'1': 'chauffage',
                      '11': 'chauffage + ecs'}

    tr006_ecs_usage = {
        '2': 'ecs',
        '11': 'chauffage + ecs'}

    td002_ch, td002_ecs = extract_type_energie_ch_ecs(td002)

    td016_ch, td016_ecs = extract_type_energie_ch_ecs(td016)

    td002_ch = td002_ch.dropna(subset=['type_energie'])
    td002_ch = td002_ch.groupby('td001_dpe_id').type_energie.apply(
        lambda x: ' + '.join(sorted(list(set((x)))))).to_frame('td002_type_energie_ch')

    td002_ecs = td002_ecs.dropna(subset=['type_energie'])
    td002_ecs = td002_ecs.groupby('td001_dpe_id').type_energie.apply(
        lambda x: ' + '.join(sorted(list(set((x)))))).to_frame('td002_type_energie_ecs')

    td016_ch = td016_ch.dropna(subset=['type_energie'])
    td016_ch = td016_ch.groupby('td001_dpe_id').type_energie.apply(
        lambda x: ' + '.join(sorted(list(set((x)))))).to_frame('td016_type_energie_ch')

    td016_ecs = td016_ecs.dropna(subset=['type_energie'])
    td016_ecs = td016_ecs.groupby('td001_dpe_id').type_energie.apply(
        lambda x: ' + '.join(sorted(list(set((x)))))).to_frame('td016_type_energie_ecs')

    td001_ener = td001[['td001_dpe_id']]
    for table in [td002_ch, td002_ecs, td016_ch, td016_ecs]:
        td001_ener = td001_ener.merge(table, on='td001_dpe_id', how='left')

    return td001_ener
