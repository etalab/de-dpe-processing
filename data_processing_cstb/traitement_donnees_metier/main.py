import pandas as pd
from pathlib import Path


def run_enveloppe_processing(td001, td006, td007, td008):
    from td007_processing import merge_td007_tr_tv, postprocessing_td007
    from td008_processing import merge_td008_tr_tv, postprocessing_td008
    from td001_merge import merge_td001_dpe_id_envelope
    from td007_processing import agg_td007_to_td001_essential, agg_surface_envelope
    from td008_processing import agg_td008_to_td001_essential

    td008_raw_cols = td008.columns
    td007_raw_cols = td007.columns

    td001, td006, td007, td008 = merge_td001_dpe_id_envelope(td001=td001, td006=td006, td007=td007, td008=td008)

    td008 = merge_td008_tr_tv(td008)
    td008 = postprocessing_td008(td008)

    td007 = merge_td007_tr_tv(td007)
    td007 = postprocessing_td007(td007, td008)

    agg_td007 = agg_td007_to_td001_essential(td007)
    agg_td008 = agg_td008_to_td001_essential(td008)
    agg_surfaces = agg_surface_envelope(td007, td008)

    td001_enveloppe_agg = pd.concat([agg_td007, agg_td008, agg_surfaces], axis=1)

    td001_enveloppe_agg.index.name = 'td001_dpe_id'
    cols = [el for el in td008.columns if el not in td008_raw_cols]
    cols.append('td008_baie_id')
    td008_p = td008[cols]
    cols = [el for el in td007.columns if el not in td007_raw_cols]
    cols.append('td007_paroi_opaque_id')
    td007_p = td007[cols]
    return td001_enveloppe_agg, td008_p, td007_p


def run_system_processing(td001, td006, td011, td012, td013, td014):
    from td011_td012_processing import merge_td012_tr_tv, postprocessing_td012, merge_td011_tr_tv, \
        agg_systeme_chauffage_essential
    from td013_td014_processing import merge_td013_tr_tv, postprocessing_td014, merge_td014_tr_tv, \
        agg_systeme_ecs_essential

    from td001_merge import merge_td001_dpe_id_system

    td011_raw_cols = td011.columns
    td012_raw_cols = td012.columns
    td013_raw_cols = td013.columns
    td014_raw_cols = td014.columns
    td001, td006, td011, td012, td013, td014 = merge_td001_dpe_id_system(td001, td006, td011, td012, td013, td014)
    td011 = merge_td011_tr_tv(td011)
    td012 = merge_td012_tr_tv(td012)
    td012 = postprocessing_td012(td012)

    cols = [el for el in td011.columns if el not in td011_raw_cols]
    cols.append('td011_installation_chauffage_id')
    td011_p = td011[cols]

    cols = [el for el in td012.columns if
            el not in td012_raw_cols + ['besoin_chauffage_infer', 'gen_ch_concat_txt_desc']]
    cols.append('td012_generateur_chauffage_id')
    td012_p = td012[cols]

    td001_sys_ch_agg = agg_systeme_chauffage_essential(td001, td011, td012)

    td014 = postprocessing_td014(td014)

    cols = [el for el in td013.columns if el not in td013_raw_cols]
    cols.append('td013_installation_ecs_id')
    td013_p = td013[cols]

    cols = [el for el in td014.columns if
            el not in td014_raw_cols + ['besoin_chauffage_infer', 'gen_ch_concat_txt_desc']]
    cols.append('td014_generateur_ecs_id')
    td014_p = td014[cols]

    td001_sys_ecs_agg = agg_systeme_ecs_essential(td001, td013, td014)

    return td011_p, td012_p, td001_sys_ch_agg, td013_p, td014_p, td001_sys_ecs_agg


if __name__ == '__main__':
    data_dir = 'D:\data\dpe_full\depts'
    for dept_dir in Path(data_dir).iterdir():
        print(dept_dir)
        # LOAD TABLES
        td007 = pd.read_csv(dept_dir / 'td007_paroi_opaque.csv', dtype=str)
        td006 = pd.read_csv(dept_dir / 'td006_batiment.csv', dtype=str)
        td001 = pd.read_csv(dept_dir / 'td001_dpe.csv', dtype=str)
        td008 = pd.read_csv(dept_dir / 'td008_baie.csv', dtype=str)
        td008 = td008.drop('td008_baie_id', axis=1)

        # ENVELOPPE PROCESSING
        td001_enveloppe_agg, td008_p, td007_p = run_enveloppe_processing(td001, td006, td007, td008)

        td001_enveloppe_agg.to_csv(dept_dir / 'td001_annexe_enveloppe_agg.csv')

        td007.to_csv(dept_dir / 'td007_paroi_opaque_annexe.csv')
        td008.to_csv(dept_dir / 'td008_baie_annexe.csv')

        # SYSTEM PROCESSING

        td011 = pd.read_csv(dept_dir / 'td011_installation_chauffage.csv', dtype=str)
        td012 = pd.read_csv(dept_dir / 'td012_generateur_chauffage.csv', dtype=str)
        td013 = pd.read_csv(dept_dir / 'td013_installation_ecs.csv', dtype=str)
        td014 = pd.read_csv(dept_dir / 'td014_generateur_ecs.csv', dtype=str)

        td011_p, td012_p, td001_sys_ch_agg, td013_p, td014_p, td001_sys_ecs_agg = run_system_processing(td001, td006,
                                                                                                        td011, td012,
                                                                                                        td013, td014)
        td001_sys_ch_agg.to_csv(dept_dir / 'td001_annexe_sys_ch_agg.csv')
        td001_sys_ch_agg.to_csv(dept_dir / 'td001_annexe_sys_ecs_agg.csv')
        td011_p.to_csv(dept_dir / 'td011_installation_chauffage_annexe.csv')
        td012_p.to_csv(dept_dir / 'td012_generateur_chauffage_annexe.csv')
        td013_p.to_csv(dept_dir / 'td013_installation_ecs_annexe.csv')
        td014_p.to_csv(dept_dir / 'td014_generateur_ecs_annexe.csv')
