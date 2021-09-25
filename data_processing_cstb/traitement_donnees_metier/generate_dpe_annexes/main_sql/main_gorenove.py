from pathlib import Path
from generate_dpe_annexes.gorenove_scripts import rename_dpe_table_light
import numpy as np
import pandas as pd
from generate_dpe_annexes.utils import round_float_cols
from multiprocessing import Pool
import pandas as pd
from pathlib import Path
import os
from generate_dpe_annexes.td001_processing import postprocessing_td001
from generate_dpe_annexes.utils import round_float_cols
from multiprocessing import Pool
from generate_dpe_annexes.sql_queries import *
from generate_dpe_annexes.config import config
from generate_dpe_annexes.td006_processing import agg_td006_td001, merge_td006_tr_tv
from generate_dpe_annexes.advanced_general_processing import main_advanced_general_processing
from generate_dpe_annexes.utils import remerge_td001_columns
import subprocess
from generate_dpe_annexes.utils import select_only_new_cols,remerge_td001_columns
import multiprocessing
import sys


#@timeit
def run_gorenove_processing(dept):
    td001 = get_td001(dept)
    gen_adv = get_annexe_table('td001_gen_agg_adv_annexe', dept=dept)
    sys_adv = get_annexe_table('td001_sys_adv_annexe', dept=dept)
    env_adv = get_annexe_table('td001_env_adv_agg_annexe', dept=dept)
    env_surf = get_annexe_table('td001_surfaces_agg_essential_annexe', dept=dept)

    td001_cols = ['td001_dpe_id', 'tv016_departement_id', 'numero_dpe', 'date_reception_dpe', 'consommation_energie',
                  'estimation_ges']

    env_surf_cols = ['td001_dpe_id', 'perc_surf_vitree_ext']

    sys_cols = ['td001_dpe_id', 'type_installation_ch', 'type_energie_ch', 'gen_ch_lib_final',
                'gen_ch_lib_principal', 'gen_ch_lib_appoint', 'is_ch_solaire', 'type_installation_ecs',
                'type_energie_ecs', 'gen_ecs_lib_final',
                'gen_ecs_lib_principal', 'gen_ecs_lib_appoint', 'is_ecs_solaire']

    gen_cols = ['td001_dpe_id', 'classe_consommation_energie', 'classe_estimation_ges', 'type_batiment', 'coherence_data_methode_dpe', 'is_3cl', 'nom_methode_dpe_norm', 'periode_construction',
                'type_ventilation',
                'inertie', 'presence_climatisation', 'enr']
    env_cols = ['td001_dpe_id', 'u_mur_ext', 'mat_mur_ext',
                'ep_mat_mur_ext', 'type_adjacence_ph', 'u_ph', 'mat_ph',
                'type_adjacence_pb', 'u_pb', 'mat_pb', 'pos_isol_mur_ext',
                'pos_isol_pb', 'pos_isol_ph', 'u_baie', 'fs_baie', 'type_vitrage_baie',
                'remplissage_baie', 'mat_baie', 'orientation_baie',
                'avancee_masque_max', 'presence_balcon', 'traversant']

    # MERGE TABLE
    grnv = td001[td001_cols].drop_duplicates('numero_dpe', keep='last')
    grnv = grnv.merge(gen_adv[gen_cols], on='td001_dpe_id', how='left')
    grnv = grnv.merge(sys_adv[sys_cols], on='td001_dpe_id', how='left')
    grnv = grnv.merge(env_surf[env_surf_cols], on='td001_dpe_id', how='left')
    grnv = grnv.merge(env_adv[env_cols], on='td001_dpe_id', how='left')
    grnv = rename_dpe_table_light(grnv, reformat_gorenove=True)
    # TODO : Suppression des libéllés indéterminés pour les aggregations logement -> batiment (a modifier lorsque la méthode d'aggregation sera affinée)
    grnv = grnv.replace('indetermine', np.nan).replace('inconnu', np.nan)

    # TEMPORARY FIX

    grnv[[col for col in grnv if 'pos' in col]] = grnv[[col for col in grnv if 'pos' in col]].replace('Non isolé', 'non isole')
    # convert bool
    grnv.presence_balcon = grnv.presence_balcon.replace({'False': False,
                                                         'True': True})
    grnv.ecs_is_solaire = grnv.ecs_is_solaire.replace({'False': False,
                                                       'True': True})
    grnv.ch_is_solaire = grnv.ch_is_solaire.replace({'False': False,
                                                     'True': True})
    dump_sql(table=grnv, table_name='td001_agg_synthese_gorenove', dept=dept)

if __name__ == '__main__':
    nb_args = len(sys.argv) - 1
    if nb_args > 0:
        config['multiprocessing']['is_multiprocessing'] = sys.argv[1]
    all_depts = get_raw_departements()
    already_processed_depts = get_annexe_departements('td001_agg_synthese_gorenove')
    depts_to_be_processed = [dept for dept in all_depts if dept not in already_processed_depts]
    if config['multiprocessing']['is_multiprocessing'] == True:

        with multiprocessing.get_context('spawn').Pool(processes=config['multiprocessing']['nb_proc']) as pool:
            pool.starmap(run_gorenove_processing, [(dept,) for dept in depts_to_be_processed])
    else:
        for dept in depts_to_be_processed:
            run_gorenove_processing(dept)


