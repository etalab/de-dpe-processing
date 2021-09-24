from generate_dpe_annexes.main_sql.main_enveloppe import run_enveloppe_processing
from generate_dpe_annexes.main_sql.main_general import run_general_processing
from generate_dpe_annexes.main_sql.main_systeme import run_systeme_processing
from generate_dpe_annexes.main_sql.main_gorenove import run_gorenove_processing
from generate_dpe_annexes.sql_queries import get_annexe_departements,get_raw_departements
from generate_dpe_annexes.config import config
import multiprocessing

def run_all(dept):
    function_name = "run_all"
    logger = config['logger']
    logger.debug(f'{function_name} -------------- init for department {dept}')

    run_enveloppe_processing(dept)
    run_systeme_processing(dept)
    run_general_processing(dept)
    run_gorenove_processing(dept)

if __name__ == '__main__':

    all_depts = get_raw_departements()
    already_processed_depts = get_annexe_departements('td001_agg_synthese_gorenove')
    depts_to_be_processed = [dept for dept in all_depts if dept not in already_processed_depts]
    if config['multiprocessing']['is_multiprocessing'] is True:
        with multiprocessing.get_context('spawn').Pool(processes=config['multiprocessing']['nb_proc']) as pool:
            pool.starmap(run_systeme_processing, [(dept,) for dept in depts_to_be_processed])