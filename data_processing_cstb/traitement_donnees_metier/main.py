import pandas as pd
from pathlib import Path
import json
from td001_processing import postprocessing_td001
from utils import round_float_cols,unique_ordered
from config import paths
def run_enveloppe_processing(td001, td006, td007, td008):
    from td007_processing import merge_td007_tr_tv, postprocessing_td007
    from td008_processing import merge_td008_tr_tv, postprocessing_td008
    from td001_merge import merge_td001_dpe_id_envelope
    from td007_processing import agg_td007_to_td001_essential, agg_surface_envelope
    from td008_processing import agg_td008_to_td001_essential

    td008_raw_cols = td008.columns.tolist()
    td007_raw_cols = td007.columns.tolist()

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
    cols = [el for el in td008.columns if el not in td008_raw_cols + ['fen_lib_from_tv009',
                                                                      'fen_lib_from_tv021']]
    cols.append('td008_baie_id')
    cols = unique_ordered(cols)
    td008_p = td008[cols]
    cols = [el for el in td007.columns if
            el not in td007_raw_cols + ["qualif_surf", 'surface_paroi_opaque_calc', 'surface_paroi_totale_calc_v1',
                                        'surface_paroi_totale_calc_v2']]
    cols.append('td007_paroi_opaque_id')
    cols = unique_ordered(cols)
    td007_p = td007[cols]
    return td001_enveloppe_agg, td008_p, td007_p


def run_system_processing(td001, td006, td011, td012, td013, td014):
    from td011_td012_processing import merge_td012_tr_tv, postprocessing_td012, merge_td011_tr_tv, \
        agg_systeme_chauffage_essential
    from td013_td014_processing import merge_td013_tr_tv, postprocessing_td014, merge_td014_tr_tv, \
        agg_systeme_ecs_essential
    from td001_merge import merge_td001_dpe_id_system

    td011_raw_cols = td011.columns.tolist()
    td012_raw_cols = td012.columns.tolist()
    td013_raw_cols = td013.columns.tolist()
    td014_raw_cols = td014.columns.tolist()
    td001, td006, td011, td012, td013, td014 = merge_td001_dpe_id_system(td001, td006, td011, td012, td013, td014)
    td011 = merge_td011_tr_tv(td011)
    td012 = merge_td012_tr_tv(td012)
    td013 = merge_td013_tr_tv(td013)
    td014 = merge_td014_tr_tv(td014)

    td012 = postprocessing_td012(td012)

    cols = [el for el in td011.columns if el not in td011_raw_cols]
    cols.append('td011_installation_chauffage_id')
    cols = unique_ordered(cols)
    td011_p = td011[cols]

    cols = [el for el in td012.columns if
            el not in td012_raw_cols + ['besoin_chauffage_infer', 'gen_ch_concat_txt_desc']]
    cols.append('td012_generateur_chauffage_id')
    cols = unique_ordered(cols)
    td012_p = td012[cols]

    td001_sys_ch_agg = agg_systeme_chauffage_essential(td001, td011, td012)

    td014 = postprocessing_td014(td013, td014)

    cols = [el for el in td013.columns if el not in td013_raw_cols]
    cols.append('td013_installation_ecs_id')
    cols = unique_ordered(cols)
    td013_p = td013[cols]

    cols = [el for el in td014.columns if
            el not in td014_raw_cols + ['score_gen_ecs_lib_infer', 'gen_ecs_concat_txt_desc']]
    cols.append('td014_generateur_ecs_id')
    cols = unique_ordered(cols)
    td014_p = td014[cols]

    td001_sys_ecs_agg = agg_systeme_ecs_essential(td001, td013, td014)

    return td011_p, td012_p, td001_sys_ch_agg, td013_p, td014_p, td001_sys_ecs_agg


def build_doc(annexe_dir):
    from doc_annexe import td001_annexe_enveloppe_agg_desc, td001_sys_ch_agg_desc, td001_sys_ecs_agg_desc, \
        td007_annexe_desc, td008_annexe_desc, td012_annexe_desc, td014_annexe_desc, enums_cstb,td001_annexe_generale_desc

    doc_annexe = dict()
    doc_annexe['td001_annexe_generale'] = td001_annexe_generale_desc
    doc_annexe['td001_annexe_enveloppe_agg'] = td001_annexe_enveloppe_agg_desc
    doc_annexe['td001_sys_ch_agg'] = td001_sys_ch_agg_desc
    doc_annexe['td001_sys_ecs_agg'] = td001_sys_ecs_agg_desc
    doc_annexe['td007_annexe'] = td007_annexe_desc
    doc_annexe['td008_annexe'] = td008_annexe_desc
    doc_annexe['td012_annexe'] = td012_annexe_desc
    doc_annexe['td014_annexe'] = td014_annexe_desc

    with open(annexe_dir / 'doc_table_annexes_cstb.json', 'w', encoding='utf-8') as f:
        json.dump(doc_annexe, f, indent=4)

    with open(annexe_dir / 'enum_table_annexes_cstb.json', 'w', encoding='utf-8') as f:
        json.dump(enums_cstb, f, indent=4)


if __name__ == '__main__':

    data_dir = paths['DPE_DEPT_PATH']
    annexe_dir = paths['DPE_DEPT_ANNEXE_PATH']
    annexe_dir = Path(annexe_dir)
    annexe_dir.mkdir(exist_ok=True, parents=True)
    build_doc(annexe_dir)
    for dept_dir in Path(data_dir).iterdir():
        print(dept_dir)
        annexe_dept_dir = annexe_dir / dept_dir.name
        annexe_dept_dir.mkdir(exist_ok=True, parents=True)
        # LOAD TABLES
        td007 = pd.read_csv(dept_dir / 'td007_paroi_opaque.csv', dtype=str)
        td006 = pd.read_csv(dept_dir / 'td006_batiment.csv', dtype=str)
        td001 = pd.read_csv(dept_dir / 'td001_dpe.csv', dtype=str)
        td008 = pd.read_csv(dept_dir / 'td008_baie.csv', dtype=str)
        td008 = td008.drop('td008_baie_id', axis=1)

        # ENVELOPPE PROCESSING
        td001_enveloppe_agg, td008_p, td007_p = run_enveloppe_processing(td001, td006, td007, td008)

        round_float_cols(td001_enveloppe_agg).to_csv(annexe_dept_dir / 'td001_annexe_enveloppe_agg.csv')
        round_float_cols(td007_p).to_csv(annexe_dept_dir / 'td007_paroi_opaque_annexe.csv')
        round_float_cols(td008_p).to_csv(annexe_dept_dir / 'td008_baie_annexe.csv')

        # SYSTEM PROCESSING

        td011 = pd.read_csv(dept_dir / 'td011_installation_chauffage.csv', dtype=str)
        td012 = pd.read_csv(dept_dir / 'td012_generateur_chauffage.csv', dtype=str)
        td013 = pd.read_csv(dept_dir / 'td013_installation_ecs.csv', dtype=str)
        td014 = pd.read_csv(dept_dir / 'td014_generateur_ecs.csv', dtype=str)

        td011_p, td012_p, td001_sys_ch_agg, td013_p, td014_p, td001_sys_ecs_agg = run_system_processing(td001, td006,
                                                                                                        td011, td012,
                                                                                                        td013, td014)
        round_float_cols(td001_sys_ch_agg).to_csv(annexe_dept_dir / 'td001_annexe_sys_ch_agg.csv')
        round_float_cols(td001_sys_ecs_agg).to_csv(annexe_dept_dir / 'td001_annexe_sys_ecs_agg.csv')
        round_float_cols(td011_p).to_csv(annexe_dept_dir / 'td011_annexe_installation_chauffage.csv')
        round_float_cols(td012_p).to_csv(annexe_dept_dir / 'td012_annexe_generateur_chauffage.csv')
        round_float_cols(td013_p).to_csv(annexe_dept_dir / 'td013_annexe_installation_ecs.csv')
        round_float_cols(td014_p).to_csv(annexe_dept_dir / 'td014_annexe_generateur_ecs.csv')

        # add td001 processing
        postprocessing_td001(td001)[['nom_methode_dpe_norm', 'id']].rename(columns={'id': 'td001_dpe_id'}).to_csv(
            annexe_dept_dir / 'td001_annexe_generale.csv')
