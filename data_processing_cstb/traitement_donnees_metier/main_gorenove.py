import zipfile
from io import BytesIO, StringIO
from pathlib import Path
from generate_dpe_annexes_scripts.gorenove_scripts import gorenove_types,rename_dpe_table_light
import json
import numpy as np
import pandas as pd
from config import paths
from generate_dpe_annexes_scripts.utils import round_float_cols
from generate_dpe_annexes_scripts.doc_annexe import gorenove_vars
from multiprocessing import Pool

data_dir = paths['DPE_DEPT_PATH']
annexe_dir = paths['DPE_DEPT_ANNEXE_PATH']
annexe_dir = Path(annexe_dir)
annexe_dir.mkdir(exist_ok=True, parents=True)
def run_postprocessing_by_depts(dept_dir):
    print(dept_dir)
    annexe_dept_dir = annexe_dir / dept_dir.name
    annexe_dept_dir.mkdir(exist_ok=True, parents=True)
    # LOAD TABLES
    td001 = pd.read_csv(dept_dir / 'td001_dpe.csv', dtype=str)
    td001 = td001.rename(columns={'id': 'td001_dpe_id'})
    gen_adv = pd.read_csv(annexe_dept_dir / 'td001_gen_agg_adv.csv', dtype=str)
    sys_adv = pd.read_csv(annexe_dept_dir / 'td001_sys_agg_adv.csv', dtype=str)
    env_surf = pd.read_csv(annexe_dept_dir / 'td001_enveloppe_surface_agg_annexe.csv', dtype=str)
    env_surf = env_surf.rename(columns={"Unnamed: 0": "td001_dpe_id"})
    env_adv = pd.read_csv(annexe_dept_dir / 'td001_env_agg_adv.csv', dtype=str)
    td001_cols = ['td001_dpe_id', 'numero_dpe', 'tr002_type_batiment_id','date_reception_dpe', 'consommation_energie', 'estimation_ges',
                  'classe_consommation_energie', 'classe_estimation_ges']

    env_surf_cols = ['td001_dpe_id', 'perc_surf_vitree_ext']

    sys_cols = ['td001_dpe_id', 'type_installation_ch', 'type_energie_ch', 'gen_ch_lib_final',
                'gen_ch_lib_principal', 'gen_ch_lib_appoint', 'is_ch_solaire', 'type_installation_ecs',
                'type_energie_ecs', 'gen_ecs_lib_final',
                'gen_ecs_lib_principal', 'gen_ecs_lib_appoint', 'is_ecs_solaire']

    gen_cols = ['td001_dpe_id', 'nom_methode_dpe_norm','periode_construction', 'type_ventilation',
                'inertie', 'presence_climatisation', 'enr']
    env_cols = ['td001_dpe_id', 'u_mur_ext', 'mat_mur_ext',
                'ep_mat_mur_ext', 'type_adjacence_ph', 'u_ph', 'mat_ph',
                'type_adjacence_pb', 'u_pb', 'mat_pb', 'pos_isol_mur_ext',
                'pos_isol_pb', 'pos_isol_ph', 'u_baie', 'fs_baie', 'type_vitrage_baie',
                'remplissage_baie', 'mat_baie', 'orientation_baie',
                'avancee_masque_max', 'presence_balcon']
    # temporary fix
    if 'periode_construction' not in gen_cols:
        gen_adv = gen_adv.merge(td001[['td01_dpe_id','annee_construction']],on='td001_dpe_id',how='left')
        periode_construction = pd.cut(gen_adv.annee_construction.astype(float),
                                      [-100000, 1800, 1948, 1970, 1988, 1999, 2005, 2012, 2020, 2100],
                                      labels=['bad inf', '<1948', '1949-1970', '1970-1988', '1989-1999', '2000-2005',
                                              '2006-2012', '>2012', 'bad sup'])
        gen_adv['periode_construction'] = periode_construction
    # MERGE TABLE
    grnv = td001[td001_cols].drop_duplicates('numero_dpe', keep='last')
    grnv = grnv.merge(gen_adv[gen_cols], on='td001_dpe_id', how='left')
    grnv = grnv.merge(sys_adv[sys_cols], on='td001_dpe_id', how='left')
    grnv = grnv.merge(env_surf[env_surf_cols], on='td001_dpe_id', how='left')
    grnv = grnv.merge(env_adv[env_cols], on='td001_dpe_id', how='left')
    grnv = rename_dpe_table_light(grnv, reformat_gorenove=True)
    # TODO : Suppression des libéllés indéterminés pour les aggregations logement -> batiment (a modifier lorsque la méthode d'aggregation sera affinée)
    grnv = grnv.replace('indetermine', np.nan).replace('inconnu', np.nan)
    round_float_cols(grnv).to_csv(annexe_dept_dir / 'td001_agg_synthese_gorenove.csv')

if __name__ == '__main__':
    # TODO : booléens en mode 0/1
    data_dir = paths['DPE_DEPT_PATH']
    annexe_dir = paths['DPE_DEPT_ANNEXE_PATH']
    annexe_dir = Path(annexe_dir)
    annexe_dir.mkdir(exist_ok=True, parents=True)

    list_dir = list(Path(data_dir).iterdir())
    firsts = [a_dir for a_dir in list_dir if
              not (annexe_dir / a_dir.name / 'td001_agg_synthese_gorenove.csv').is_file()]
    lasts = [a_dir for a_dir in list_dir if (annexe_dir / a_dir.name / 'td001_agg_synthese_gorenove.csv').is_file()]
    print(len(firsts), len(lasts))
    list_dir = firsts + lasts
    #list_dir = [el for el in list_dir if '94' in el.name]
    list_dir.reverse()
    # for dept_dir in list_dir:
    #     if dept_dir.name == '94':
    #         print(dept_dir)
    #         run_postprocessing_by_depts(dept_dir)

    with Pool(processes=3) as pool:
        pool.starmap(run_postprocessing_by_depts, [(dept_dir,) for dept_dir in list_dir])

    # cat_variables = [k for k, v in gorenove_types.items() if v == 'category']
    #
    # json_meta_gorenove = dict()
    #
    # json_meta_gorenove['version'] = '3.0.0'
    # json_meta_gorenove['types'] = gorenove_types
    # json_meta_gorenove['enums'] = {k: [] for k in cat_variables}
    # json_meta_gorenove['doc_vars'] = gorenove_vars
    # root_dir = Path(r'D:\data\dpe_full')
    # depts_dir = root_dir / Path('annexes_cstb')
    # td001_zipped_dir = root_dir / Path('annexes_cstb_zipped_for_gorenove')
    # td001_zipped_dir.mkdir(exist_ok=True)
    # for dept in depts_dir.iterdir():
    #     if dept.is_dir():
    #         zipped_file = td001_zipped_dir / f'{dept.name}.zip'
    #         mf = BytesIO()
    #         with zipfile.ZipFile(mf, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
    #             for a_file in dept.iterdir():
    #                 if a_file.name.startswith('td001_gorenove'):
    #                     table = pd.read_csv(a_file, index_col=0, dtype=str)
    #                     table = table.replace('nan', np.nan)
    #                     table = table.rename(columns={'u_mur_ext_y': 'u_mur_ext'})
    #                     for col in cat_variables:
    #                         json_meta_gorenove['enums'][col] += table[col].dropna().unique().tolist()
    #                         json_meta_gorenove['enums'][col] = list(set(json_meta_gorenove['enums'][col]))
    #                     zf.writestr('td001_agg_synthese_gorenove.csv', table.to_csv())
    #
    #         with open(zipped_file, "wb") as f:  # use `wb` mode
    #             f.write(mf.getvalue())
    # with open(td001_zipped_dir/'meta_data.json','w') as f:
    #     json.dump(json_meta_gorenove,f,indent=4)
    #
