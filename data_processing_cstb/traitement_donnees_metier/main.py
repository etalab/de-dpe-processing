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
    td008 = merge_td008_tr_tv(td008)
    td008 = postprocessing_td008(td008)

    td007 = merge_td007_tr_tv(td007)
    td007 = postprocessing_td007(td007, td008)

    td001, td006, td007, td008 = merge_td001_dpe_id_envelope(td001=td001, td006=td006, td007=td007, td008=td008)

    agg_td007 = agg_td007_to_td001_essential(td007)
    agg_td008 = agg_td008_to_td001_essential(td008)
    agg_surfaces = agg_surface_envelope(td007, td008)

    agg = pd.concat([agg_td007, agg_td008, agg_surfaces], axis=1)

    agg.index.name = 'td001_dpe_id'
    cols = [el for el in td008.columns if el not in td008_raw_cols]
    cols.append('td008_baie_id')
    td008_p = td008[cols]
    cols = [el for el in td007.columns if el not in td007_raw_cols]
    cols.append('td007_paroi_opaque_id')
    td007_p = td007[cols]
    return agg, td008_p, td007_p


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
        agg, td008_p, td007_p = run_enveloppe_processing(td001, td006, td007, td008)
        agg.to_csv(dept_dir / 'td001_annexe_enveloppe_agg.csv')
        td007.to_csv(dept_dir / 'td007_paroi_opaque_annexe.csv')
        td008.to_csv(dept_dir / 'td008_baie_annexe.csv')
