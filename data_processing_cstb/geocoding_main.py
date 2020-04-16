import pandas as pd
import geopandas
import json
import requests
from io import StringIO
import traceback as tb
from pathlib import Path
import numpy as np
import geopandas as gpd
import sys
import requests
import json
from pathlib import Path
from io import StringIO, BytesIO
import pandas as pd
import uuid
import geopandas as gpd
import traceback as tb
import uuid
import contextily as ctx

from ban_utils import *
from addr_utils import *
from addr_viz import *


def cleanup_etalab_csv_files(dpe_table):
    # temporary etalab postprocessing
    dpe_table.index.name = 'id'
    dpe_table = dpe_table.reset_index()
    dpe_table['id'] = dpe_table['id'].astype(str)
    res_cols_etalab = [col for col in dpe_table if col.startswith('result_')]
    dpe_table = dpe_table.rename(columns={col: col + '_etalab' for col in res_cols_etalab})
    dpe_table['result_citycode_etalab'] = dpe_table['result_citycode_etalab'].str.replace('\.0', '')
    dpe_table['result_postcode_etalab'] = dpe_table['result_postcode_etalab'].str.replace('\.0', '')
    return dpe_table


def cleanup_dpe_table(dpe_table):
    dpe_table['code_insee'] = dpe_table.code_insee_commune_actualise

    codes = ['code_insee', 'code_insee_commune', 'code_postal', 'code_insee_commune_actualise']
    for code in codes:
        dpe_table[code] = dpe_table[code].astype(str).str.replace('\.0', '').str.zfill(5)

    dpe_table = dpe_table.drop_duplicates('numero_dpe', keep='last')

    na_terms = {'Non communiqué': np.nan,
                "NC": np.nan,
                "nan":np.nan,
                }

    dpe_table = dpe_table.replace(na_terms)
    return dpe_table


def batch_geocoding(dpe_addr, geocode_cols, geocode_output_dir, dept):
    # GEOCODING BATCH

    path_geo = geocode_output_dir / f'addr_dpe_{dept}.h5'

    # recall previous geocoding if already exists and will recover missing address
    if Path(path_geo).is_file():
        # using HDFSTORE because preserve types and perf.
        with pd.HDFStore(path_geo, 'r') as store:
            data_out = store['addr_dpe']
    else:
        data_out = None

    # run addok /search/csv by chunks because big files tends to fail more.
    data_out = run_get_addok_search_csv_by_chunks(data=dpe_addr, geocode_cols=geocode_cols,
                                                  addr_cols=['adresse_concat'],
                                                  data_out=data_out, addok_search_csv_url=addok_search_csv_url)

    # using HDFSTORE because preserve types and perf.
    with pd.HDFStore(path_geo, 'w') as store:
        store['addr_dpe'] = data_out


def load_geocoded_table(dept, geocode_output_dir):
    path_geo = geocode_output_dir / f'addr_dpe_{dept}.h5'
    with pd.HDFStore(path_geo, 'r') as store:
        dpe_geo = store['addr_dpe']

    return dpe_geo


def select_best_geocoding_result(dpe_geo, bonus_match_code=0.1):
    """
    select best geocoding results using theses 4 ordered criteria

    crit 1 : good match of departement between result and input

    crit 2 : at least code postal or code insee are matching with result -> changed to a bonus point to final score

    crit 3 : priority of results  'housenumber' and 'locality'

    crit 4 : finally score.

    Parameters
    ----------
    dpe_geo :pd.DataFrame
    dpe table geocoded

    Returns
    -------

    """

    dpe_geo['dept_from_cp'] = dpe_geo.code_postal.apply(lambda x: x[0:2] if isinstance(x, str) else x)
    dpe_geo['dept_from_code'] = dpe_geo.code_insee.apply(lambda x: x[0:2] if isinstance(x, str) else x)
    dpe_geo['result_dept'] = dpe_geo.result_citycode.apply(lambda x: x[0:2] if isinstance(x, str) else x)

    dpe_geo['match_dept'] = (dpe_geo.result_dept == dpe_geo.dept_from_cp)
    dpe_geo['match_dept'] = dpe_geo['match_dept'] | (dpe_geo.result_dept == dpe_geo.dept_from_code)

    # on regarde si l'adresse trouvée est dans la commune source déclarée à la base.
    bool_citycode = dpe_geo.result_citycode == dpe_geo.code_insee
    bool_postcode = dpe_geo.result_postcode == dpe_geo.code_postal
    match_codes = bool_citycode | bool_postcode
    dpe_geo['match_codes'] = match_codes
    dpe_geo['result_score_adj'] = dpe_geo.result_score.astype(float) + match_codes * bonus_match_code
    dpe_geo['result_type'] = pd.Categorical(dpe_geo.result_type,
                                            categories=['housenumber', 'locality', 'street', 'municipality'],
                                            ordered=True)
    #     dpe_geo = dpe_geo.sort_values(['match_dept', 'match_codes', 'result_type', 'result_score'],
    #                                   ascending=[False, False, True, False]).drop_duplicates(subset=['id'])

    dpe_geo = dpe_geo.sort_values(['match_dept', 'result_type', 'result_score_adj'],
                                  ascending=[False, True, False]).drop_duplicates(subset=['id'])

    return dpe_geo


def generate_rolling_addr_comb(s_id_addr, s_addr_without_com, min_words=3):
    """
    EXPERIMENTAL : using rolling words in addr text to filter bad denomination at the end or begining of address
    "RESIDENCE DES ROSIERS 34 AV. GEORGES CLEMENCEAU" will generate as one of the comb "34 Av. GEORGES CLEMENCEAU" and remove
    "REISDENCE DES ROSIERS" that make the geocoding fail.

    WARNING : this generate a lot of possibilities and should be used only to recover already identified pathological cases.(fails)

    Parameters
    ----------
    s_id_addr : pd.Series
    ids
    s_addr_without_com : pd.Series
    serie containing the address field without the town.
    min_word : int
    minimum number of words to constitute an address.

    Returns
    -------

    """
    res_list = list()
    for (id_, addr_txt) in zip(s_id_addr, s_addr_without_com):

        txt_split = addr_txt.split(' ')
        max_words = len(txt_split)

        for n_words in range(min_words, max_words):
            for start in range(max_words - n_words + 1):
                addr_concat = (' '.join(txt_split[start:n_words + start])).strip()
                res_list.append({'adresse_concat_without_com': addr_concat,
                                 "id_addr": id_})

    df_comb = pd.DataFrame(res_list)
    return df_comb


def experimental_recover_bad_addr(dpe_geo, dpe_addr, experimental_recover_output_dir, dept):
    dpe_addr_to_recover = \
        dpe_geo.loc[dpe_geo.not_geocoded].dropna(subset=['id', 'adresse_concat_without_com', 'commune'])[
            dpe_addr.columns]

    # .fillna('')

    dpe_addr_comb_rec = generate_rolling_addr_comb(dpe_addr_to_recover.id_addr,
                                                   dpe_addr_to_recover.adresse_concat_without_com)

    dpe_addr_comb_rec = dpe_addr_comb_rec.merge(
        dpe_addr_to_recover.drop(['adresse_concat_without_com', 'adresse_concat'], axis=1), on='id_addr', how='left')

    dpe_addr_comb_rec['adresse_concat'] = build_concat_addr_from_table(dpe_addr_comb_rec,
                                                                       ['adresse_concat_without_com', 'commune'])

    path_recover_output = experimental_recover_output_dir / f'addr_dpe_{dept}.h5'

    geocode_cols = ['id_addr', 'adresse_concat']

    if Path(path_recover_output).is_file():
        with pd.HDFStore(path_recover_output, 'r') as store:
            data_out = store['addr_dpe']
    else:
        data_out = None

    data_out = run_get_addok_search_csv_by_chunks(data=dpe_addr_comb_rec, geocode_cols=geocode_cols,
                                                  addr_cols=['adresse_concat'],
                                                  data_out=data_out)

    with pd.HDFStore(path_recover_output, 'w') as store:
        store['addr_dpe'] = data_out

    with pd.HDFStore(path_recover_output, 'r') as store:
        dpe_geo_err = store['addr_dpe']

    dpe_geo_err = dpe_geo_err.merge(dpe_addr.drop(set(geocode_cols) - {'id_addr'}, axis=1), on='id_addr', how='left')

    dpe_geo = dpe_geo.append(dpe_geo_err, ignore_index=True)

    return dpe_geo


def main(res_dir):


    res_dir = Path(res_dir)
    res_dir.mkdir(exist_ok=True, parents=True)

    source_dir = path_d / 'data' / 'dpe' / 'upload'

    # path to write geocoded files
    geocode_output_dir = Path(res_dir) / 'geocode_output'
    geocode_output_dir.mkdir(exist_ok=True, parents=True)
    # path to write experimental recover addr files files
    experimental_recover_output_dir = Path(res_dir) / 'experimental_recover_output'
    experimental_recover_output_dir.mkdir(exist_ok=True, parents=True)
    # path for file matching raw commune name with ban
    match_ban_name_com_dir = Path(res_dir) / 'match_ban_name_com'
    match_ban_name_com_dir.mkdir(exist_ok=True, parents=True)

    # path for final output table
    final_output_dir = Path(res_dir) / 'final_output'
    final_output_dir.mkdir(exist_ok=True, parents=True)

    # ADDOK and BAN URL
    addok_search_csv_url = 'https://api-adresse.data.gouv.fr/search/csv'
    communes_dep_url = 'https://geo.api.gouv.fr/departements/{code}/communes?format=geojson'
    communes_dep_url_center = 'https://geo.api.gouv.fr/departements/{code}/communes?format=geojson&geometry=center'
    communes_dep_url_light = 'https://geo.api.gouv.fr/departements/{code}/communes'
    addok_search_url = "https://api-adresse.data.gouv.fr/search/?q="
    ban_addr_dept_url = 'https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/adresses-{dept}.csv.gz'
    dept_geocoded = [el.name.split('_')[2].replace('.h5','') for el in geocode_output_dir.iterdir()]
    source_tuple = [(source_file,source_file.name.split('-')[1].zfill(2)) for source_file in source_dir.iterdir()]
    firsts = [el for el in source_tuple if el[1] not in dept_geocoded]
    lasts = [el for el in source_tuple if el[1] in dept_geocoded]
    source_tuple = firsts + lasts
    for source_file,dept in source_tuple:

        print(f'BEGINING GEOCODING DEPT : {dept}')
        td001_dpe_file = source_file

        # LOAD CLEANUP TABLE
        dpe_table = pd.read_csv(td001_dpe_file, sep=',', error_bad_lines=False, dtype=str, index_col=0)
        dpe_table = cleanup_etalab_csv_files(dpe_table)
        dpe_table = cleanup_dpe_table(dpe_table)

        # GET COMMUNES META DATA
        com_ban_geo = get_communes_table(dept, communes_dep_url=communes_dep_url)  # COMMUNES LIST OF DEPT
        df_cp_com_flat = build_communes_cp_table_flat(com_ban_geo)  # FLAT CORRESPONDANCE CP/CODEINSEE

        # BUILD ADDR FIELDS
        addr_concat_cols = ['numero_rue', 'type_voie', 'nom_rue']
        dpe_table['adresse_concat_without_com'] = build_concat_addr_from_table(dpe_table, addr_concat_cols)
        addr_table_cols = ['id', 'adresse_concat_without_com', 'code_insee', 'code_postal', 'commune']
        dpe_addr = dpe_table[addr_table_cols].copy()  # select only useful geocoding columns

        # BUILD POSSIBLE COMMUNE FIELDS
        # RAW commune field
        dpe_addr_raw = dpe_addr.copy()
        dpe_addr_raw['com_source'] = 'raw'

        # MATCH NAME OF RAW COMMUNE WITH ADDOK SEARCH ENGINE (txt association)
        if not Path(match_ban_name_com_dir / f'match_ban_name_com_{dept}.csv').is_file():
            # association table Raw commune <-> BAN commune
            df_match_ban_name_com = addok_search_match_commune(dpe_table['commune'], dept=dept,
                                                               addok_search_url=addok_search_url)
            df_match_ban_name_com.to_csv(match_ban_name_com_dir / f'match_ban_name_com_{dept}.csv')
        else:
            df_match_ban_name_com = pd.read_csv(match_ban_name_com_dir / f'match_ban_name_com_{dept}.csv', dtype=str, index_col=0)

        # BUILD all communes possibilities for each address using raw commune txt or cp or code insee.
        # duplicates entry of address for same dpe id (will take only the best geocoding result later)
        dpe_addr = build_communes_possibilities_using_ban(dpe_addr, df_match_ban_name_com, df_cp_com_flat)

        # CONCAT addr with commune
        dpe_addr['adresse_concat'] = build_concat_addr_from_table(dpe_addr,
                                                                  ['adresse_concat_without_com', 'commune'])

        # Creating a variant with code postal at the end provide sometimes better geocoding results
        dpe_addr_cp = dpe_addr.copy()
        dpe_addr_cp['adresse_concat'] = build_concat_addr_from_table(dpe_addr_cp,
                                                                     ['adresse_concat_without_com', 'commune',
                                                                      'code_postal'])
        #        # Creating a variant with code insee at the end provide sometimes better geocoding results
        #         dpe_addr_code = dpe_addr.copy()
        #         dpe_addr_code['adresse_concat'] = build_concat_addr_from_table(dpe_addr_code,
        #                                                                      ['adresse_concat_without_com', 'commune',
        #                                                                       'code_insee'])

        dpe_addr = pd.concat([dpe_addr, dpe_addr_cp])
        #        dpe_addr = pd.concat([dpe_addr, dpe_addr_cp,dpe_addr_code])

        dpe_addr = dpe_addr.sort_values(["id", 'adresse_concat', 'code_insee', 'code_postal']).drop_duplicates(
            subset=["id", 'adresse_concat', 'code_insee', 'code_postal'])

        dpe_addr['id_addr'] = range(0, len(dpe_addr))
        dpe_addr.id_addr = dpe_addr.id_addr.astype(str)
        dpe_addr['id_addr'] = dpe_addr['id'] + '_' + dpe_addr['id_addr']

        geocode_cols = ['id_addr',
                        'adresse_concat']  # not including postcode, citycode because feature is bugged on addok.
        batch_geocoding(dpe_addr=dpe_addr, geocode_cols=geocode_cols, geocode_output_dir=geocode_output_dir,
                        dept=dept)

        dpe_geo = load_geocoded_table(dept, geocode_output_dir=geocode_output_dir)
        dpe_geo = dpe_geo.merge(dpe_addr.drop(set(geocode_cols) - {'id_addr'}, axis=1), on='id_addr', how='right')
        dpe_geo = select_best_geocoding_result(dpe_geo)
        dpe_cols = list(set(dpe_table.columns) - set(dpe_geo))
        dpe_geo = dpe_geo.merge(dpe_table[dpe_cols + ['id']], on='id', how='right')

        path_geo = final_output_dir / f'dpe_{dept}_full_geocoded.h5'
        dpe_geo['result_type'] = dpe_geo['result_type'].astype(str) # remove categorical dtype for result_type
        # TODO : filter only useful cols
        with pd.HDFStore(path_geo, 'w') as store:
            store.put('td001_geocoded', dpe_geo)

if __name__ == '__main__':
    if sys.platform == 'linux':
        path_d = Path('/mnt/d')
    else:
        path_d = Path('D://')

    res_dir = path_d / 'test' / 'base_dpe_geocode'
    res_dir.mkdir(exist_ok=True, parents=True)
    main(res_dir)
