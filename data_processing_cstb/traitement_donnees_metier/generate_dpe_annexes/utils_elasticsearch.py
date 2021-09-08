from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import time
import uuid
import pandas as pd

sleepers = {'after_bulk': 1,
            'after_search': 0.1}


def setup_es_client(index_name):
    es_client = Elasticsearch(hosts=['es01', 'es02'])
    index_name = index_name
    index_configurations = {
        "settings": {
            "index": {
                "number_of_shards": 4,
                "number_of_replicas": 0,
                "max_result_window": 5000000,

            }
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "text_to_analyze": {
                    "type": "text",
                    # "analyzer": "french"
                },
                "category_index": {
                    'type': 'keyword'
                }
            }
        }
    }

    try:
        es_client.indices.delete(
            index=index_name,
        )
    except:
        pass
    es_client.indices.create(
        index=index_name,
        body=index_configurations
    )
    return es_client


def gendata(index_name, id_data, data):
    for id_, vr in zip(id_data, data):
        yield {
            "_index": index_name,
            "id": id_,
            "text_to_analyze": vr
        }


def generate_instruction_from_list(char_list):
    new_char_list = list()
    for char in char_list:
        if isinstance(char, str):
            new_char_list.append(char)
        elif isinstance(char, tuple):
            new_char_list.append('(' + ') OR ('.join(char) + ')')

    search_instruction = '(' + ') AND ('.join(new_char_list) + ')'
    search_instruction = search_instruction.replace('AND (NOT) AND', 'AND NOT')
    search_instruction = search_instruction.replace('(NOT) AND', 'NOT')

    return search_instruction


def search_from_search_dict(es_client, search_dict, index_name):
    s_list = list()
    for label, char_list in search_dict.items():
        time.sleep(sleepers['after_search'])
        search_instruction = generate_instruction_from_list(char_list)
        search_body = {
            "query": {
                "query_string": {
                    "query": search_instruction,
                    "default_field": "text_to_analyze"
                },

            },

        }
        # es_client.count(index=index_name, body=search_body)
        a_dict = es_client.search(index=index_name, body=search_body, size=5000000, request_timeout=60 * 5)

        hits = a_dict['hits']['hits']

        s = pd.Series(index=[el['_source']['id'] for el in hits], dtype='str')
        s[:] = label
        s_list.append(s)
    s_all = pd.concat(s_list)
    return s_all


def search_and_affect(data_to_search, id_col, val_col, search_dict):
    index_name = uuid.uuid4()
    es_client = setup_es_client(index_name)
    L = data_to_search.shape[0]
    bulk(es_client, gendata(index_name, data_to_search[id_col], data_to_search[val_col]))
    count_r = es_client.count(index=index_name, request_timeout=60 * 5)['count']

    while L != count_r:
        time.sleep(0.1)
        count_r = es_client.count(index=index_name, request_timeout=60 * 5)['count']

    res_serie = search_from_search_dict(es_client, search_dict, index_name=index_name)

    res_table = res_serie.to_frame('label')

    res_table.index.name = id_col
    res_table = res_table.reset_index()
    # destroy index a la fin
    try:
        es_client.indices.delete(
            index=index_name,
        )
    except:
        pass

    # #     grp = df_drop.groupby('id').label.apply(lambda x: ' + '.join(sorted(list(set(x))))).reset_index()
    # #     m=data.merge(grp,on='id',how='left')

    #     m.label=m.label.fillna('indetermine')
    return res_table


def categorize_search_res(table, label_cat=None, category_dict=None):
    if category_dict is not None:
        table['category'] = table.label.replace(category_dict)

    if label_cat is not None:
        table['label'] = pd.Categorical(table.label, categories=label_cat, ordered=True)

    # df_drop = df.sort_values('label').drop_duplicates(subset=['id','category'])

    table.label = table.label.fillna('indetermine')
    return table
