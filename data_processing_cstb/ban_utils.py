import requests
import json
from pathlib import Path
from io import StringIO,BytesIO
import pandas as pd
import uuid
import geopandas as gpd
import traceback as tb
from gzip import decompress
addok_search_csv_url = 'https://api-adresse.data.gouv.fr/search/csv'
communes_dep_url = 'https://geo.api.gouv.fr/departements/{code}/communes?format=geojson'
communes_dep_url_center = 'https://geo.api.gouv.fr/departements/{code}/communes?format=geojson&geometry=center'
communes_dep_url_light ='https://geo.api.gouv.fr/departements/{code}/communes'
addok_search_url="https://api-adresse.data.gouv.fr/search/?q="
ban_addr_dept_url='https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/adresses-{dept}.csv.gz'



def get_addok_search(string, postcode=None, rqst_type=None, latlon=None, addok_search_url=addok_search_url):
    """
    call to  https://geo.api.gouv.fr/adresse -> /search and return deserialized json output.
    Parameters
    ----------
    string : str
    text to be searched by addok_search
    postcode : str(optional)
    postcode associated to the search string
    rqst_type: str
    type for addok search street,housenumber,locality,municipality
    latlon: tuple
    tuple containing latitude, longitude around which to search.
    addok_search_url :str
    url of addok search route.

    Returns
    -------

    """
    if postcode:
        string += f'&postcode={postcode}'
    if rqst_type:
        string += f'&type={rqst_type}'
    if latlon:
        lat, lon = latlon
        string += f'&lat={lat}&lon={lon}'

    try:
        r = requests.get(addok_search_url + string)
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

    list_features = json.loads(r.content)['features']
    return list_features

def get_addok_search_csv(df_addr, addr_cols, citycode_col=None, postcode_col=None,
                          addok_search_csv_url=addok_search_csv_url, keep_debug_file=False,debug_file_path=None,
                         return_bad_csv_output=False):
    """
    call to  https://geo.api.gouv.fr/adresse -> /search/csv and return deserialized json output.

    Parameters
    ----------
    df_addr :pd.DataFrame
    dataframe containing address fields to be geocoded by addok search csv
    addr_cols :list
    list of address fields (can be only one)
    citycode_col : str (opt)
    column name containing citycode(insee)
    postcode_col : str(opt)
    column name containing postcode
    addok_search_csv_url : str
    url of addok search csv route.
    keep_debug_file : bool(default False)
    debug_file_path  :str(opt)
    if provided will dump debug file in path else will dump uuid based file.
    return_bad_csv_output : bool (default True)
    if True returns incomplete bad csv output (with missing elements.)

    Returns
    -------

    """
    if isinstance(addr_cols, str):
        addr_cols = [addr_cols]
    is_temp_file = False
    # generate temp file if no path specified
    if debug_file_path is None:
        is_temp_file = True
        debug_file_path = Path(f'./temp_{str(uuid.uuid4())}.csv')

    debug_file_path = Path(debug_file_path)

    # config post request
    post_data = {'columns': tuple(addr_cols)}
    if citycode_col is not None:
        post_data.update({'citycode': citycode_col})
    if postcode_col is not None:
        post_data.update({'postcode': postcode_col})

    try:
        # dump file
        mem_file = BytesIO()
        mem_file.write(df_addr.to_csv(encoding='utf-8', index=False).encode())
        mem_file.seek(0)
        df_shape = df_addr.shape
        files = {
            'data': ('file', mem_file),
        }
        r = requests.post(addok_search_csv_url, files=files, data=post_data)
        r.raise_for_status()

        f = StringIO(r.content.decode())
        df_out = pd.read_csv(f, dtype=str)
        expected_shape = (df_shape[0], df_shape[1] + 16)
        # if output data has not expected shape -> geocoding failed with addok
        if df_out.shape != expected_shape:

            # if output data has proper length of columns (meaning good enough response)
            # return incomplete file even if incomplete.
            if df_out.shape[1] == expected_shape[1]:
                print(f'bad output csv shape : {df_out.shape} expected {expected_shape} returning incomplete file')

                return df_out
            exc_txt = f'addok csv service  {addok_search_csv_url} didnt geocode data properly for input file input file shape {df_shape} expected output file shape {expected_shape} but got {df_out.shape} instead'
            if not keep_debug_file and is_temp_file:
                if debug_file_path.is_file():
                    debug_file_path.unlink()
            if return_bad_csv_output is True:
                print(exc_txt)

                return df_out
            else:
                raise Exception(exc_txt)
        # if temp file remove
        if is_temp_file:
            if debug_file_path.is_file():
                debug_file_path.unlink()
        return df_out

    except requests.exceptions.HTTPError as err:
        # if temp file remove if not debug
        if not keep_debug_file and is_temp_file:
            if debug_file_path.is_file():
                debug_file_path.unlink()
        raise Exception(err)

    except Exception as e:
        # if temp file remove if not debug
        if not keep_debug_file and is_temp_file:
            if debug_file_path.is_file():
                debug_file_path.unlink()
        raise e

# In[8]:

def addok_search_match_commune(s_com, dept,addok_search_url=addok_search_url):
    communes = s_com.dropna().unique()
    communes_list = list()
    for commune in communes:
        # on utilise la fonction de matching du moteur adresse d'etalab sur les communes
        list_features = get_addok_search(commune,addok_search_url)
        # on scinde en réponse commune/réponse adresse
        list_com = [el for el in list_features if el['properties']['type'] == 'municipality'
                    and el['properties']['citycode'].startswith(dept)]
        list_addr = [el for el in list_features if el['properties']['type'] != 'municipality'
                     and el['properties']['citycode'].startswith(dept)]
        # si une commune unique matche on prend cette commune
        if len(list_com) == 1:
            com = list_com[0]['properties']
            com['com_name_dpe'] = commune
            com['match_ban_com_name_score'] = com['score']
            com['match_ban_com_name_status'] = 'commune unique'

            communes_list.append(com)
        # si on a pas de commune on prend la top freq de la commune sur la liste d'adresse.
        # si égalité on prend le score de matching max le plus haut.
        elif len(list_com) == 0 and len(list_addr) > 0:
            list_addr = [el['properties'] for el in list_addr]
            top_com = pd.DataFrame(list_addr)

            top_com = pd.concat([top_com.city.value_counts(), top_com.groupby('city').score.max()], axis=1)

            top_com.columns = ['city_count', 'max_score']

            top_com = top_com.sort_values(['city_count', 'max_score'], ascending=False).index[0]
            list_features = get_addok_search(top_com)
            list_com = [el for el in list_features if el['properties']['type'] == 'municipality'
                        and el['properties']['citycode'].startswith(dept)]
            com = list_com[0]['properties']
            com['com_name_dpe'] = commune
            com['match_ban_com_name_score'] = com['score']
            com['match_ban_com_name_status'] = 'liste adresses'
            communes_list.append(com)

        elif len(list_com) == 0:
            pass
        # si multi communes on prend celle avec le meilleur score de matching.
        else:
            list_com = [el['properties'] for el in list_com]
            com = pd.DataFrame(list_com).sort_values('score', ascending=False).iloc[0].to_dict()
            com['com_name_dpe'] = commune
            com['match_ban_com_name_status'] = 'commune multiple'
            com['match_ban_com_name_score'] = com['score']
            communes_list.append(com)

    df_com_match = pd.DataFrame(communes_list)

    df_com_match = df_com_match.rename(columns={'postcode': 'code_postal',
                                                'citycode': 'code_insee',
                                                'name': 'nom_com_ban_name',
                                                'com_name_dpe': 'commune'})

    cols = ['nom_com_ban_name', 'commune', 'match_ban_com_name_score', 'match_ban_com_name_status', 'code_postal',
            'code_insee']
    if len(set(cols)-set(df_com_match.columns))==0:
        df_com_match = df_com_match[cols]
    else:
        df_com_match = pd.DataFrame(columns=cols)
    return df_com_match


# In[9]:


def get_communes_table(dept, geometry='contour', communes_dep_url=communes_dep_url):
    communes_dep_url += f'&geometry={geometry}'

    r = requests.get(communes_dep_url.format(code=dept))

    com_ban_geo = json.loads(r.content)  # load commune ban for dept

    com_ban_geo = gpd.GeoDataFrame.from_features(com_ban_geo, crs='epsg:4326')

    return com_ban_geo


# In[10]:


def build_communes_cp_table_flat(com_ban_geo):
    cp_to_commune = list()
    for el in com_ban_geo.to_dict('records'):

        for cp in el['codesPostaux']:
            el = el.copy()
            el['code_postal'] = cp
            cp_to_commune.append(el)

    # generation d'une table flat many to many code_insee,code_postal
    df_cp_com_flat = pd.DataFrame(cp_to_commune)
    df_cp_com_flat = df_cp_com_flat.rename(columns={'code': 'code_insee',
                                                    'nom': 'nom_commune'})

    # ajout d'un booleen pour savoir si un cp correspond à plusieurs commune
    one_cp_many_com = df_cp_com_flat.groupby('code_postal').code_insee.count().to_frame('one_cp_many_com')
    one_cp_many_com = one_cp_many_com > 1

    df_cp_com_flat = df_cp_com_flat.merge(one_cp_many_com.reset_index(), on='code_postal', how='left')
    return df_cp_com_flat

def run_get_addok_search_csv_by_chunks(data, geocode_cols,addr_cols,id_addr_col='id_addr',search_kwargs=None,n_chunk=1000,n_retry_max=3,data_out=None,addok_search_csv_url=addok_search_csv_url):
    if search_kwargs is None:
        search_kwargs={}
    if id_addr_col not in data:
        raise Exception('need to have an id for address')
    geocode_cols = list(set(geocode_cols+[id_addr_col]))
    if data_out is None:
        data_out = pd.DataFrame()
    else:
        rest = set(data[id_addr_col].unique()) - set(data_out[id_addr_col].unique())
        data = data.loc[data[id_addr_col].isin(rest)].sample(frac=1)
    try:
        list_chunk = [data.iloc[i:i + n_chunk][geocode_cols] for i in range(0, data.shape[0]+n_chunk, n_chunk)]
        rest = set(data[id_addr_col].unique())
        last_rest = {}
        n_retry = 0
        while rest != last_rest or n_retry < n_retry_max:
            last_rest = rest
            print('===============RETRY============================')
            print(f'==============={n_retry}============================')
            print(len(rest), len(last_rest))
            if len(list_chunk) > 0:
                for i, chunk in enumerate(list_chunk):
                    retry = 0

                    if chunk.shape[0]>0:
                        status = 'failed'
                        while retry < 1:
                            print(f'{i} chunk started....')

                            try:
                                print(f'query {addok_search_csv_url}')

                                chunk_out = get_addok_search_csv(df_addr=chunk, addr_cols=addr_cols,
                                                                 **search_kwargs)
                                data_out = data_out.append(chunk_out, ignore_index=True)
                                retry = 1000
                                status = 'success'
                            except Exception as e:

                                print('error')
                                print(i)
                                print(tb.format_exc())
                            retry += 1
                        print(status)
            if data_out.shape[0]>0:
                rest = set(data[id_addr_col].unique()) - set(data_out[id_addr_col].unique())
                data = data.loc[data[id_addr_col].isin(rest)].sample(frac=1)
                list_chunk = [data.iloc[i:i + n_chunk][geocode_cols] for i in range(0, data.shape[0]+n_chunk, n_chunk)]
            n_retry += 1
    except Exception as e:
        print(tb.format_exc())

        print(e)
    finally:
        return data_out


def get_ban_addr_dept(dept, path=None):
    r = requests.get(ban_addr_dept_url.format(dept=dept))
    r.raise_for_status()
    if path is not None:
        with open(path, 'wb') as f:
            f.write(r.content)

    return r.content


def deserialize_ban_addr(content_or_path):
    if Path(content_or_path).is_file():
        ban_addr = pd.read_csv(content_or_path, sep=';', dtype=str)

    elif len(content_or_path) > 10000:
        bio = BytesIO(decompress(content_or_path))
        ban_addr = pd.read_csv(bio, sep=';', dtype=str)
    else:
        raise Exception('bad content or path')

    return ban_addr