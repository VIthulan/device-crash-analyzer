import csv, json
import numpy as np
import pandas
from elasticsearch import Elasticsearch

es_server = 'HOST_URL'
es_port = 80


def connect_to_es(es_server, es_port):
    elastic_client = Elasticsearch(
        [{'host': es_server, 'port': es_port}])
    if elastic_client.ping():
        print('Yay Connect')
        return elastic_client
    else:
        print('Awww it could not connect!')
        return None


es_connect = connect_to_es(es_server, es_port)


def get_index_size(es_connection, indices):
    if es_connection is not None:
        return es_connection.count(index=indices)['count']


def get_required_loops(es_connection, index_name, size):
    index_data_count = get_index_size(es_connection, [index_name])
    required_loops = index_data_count // size
    if index_data_count % size > 0:
        required_loops = required_loops + 1
    return required_loops


def download_all_data(es_connection, index_name, size):
    pass


def download_all_with_offset(es_connection, index_name, start_index, size):
    result = es_connection.search(index=index_name, doc_type="_doc",
                                  body={'size': size, 'from': start_index})


def download_all_with_offset_and_key(es_connection, index_name, start_index, size, search_item):
    if es_connection is None:
        return None
    return es_connection.search(index=index_name, doc_type="_doc",
                                body={'size': size, 'from': start_index,
                                      'query': {"term": {search_item}}})


def download_all_with_key(es_connection, index_name, size, search_item):
    elastic_docs = []
    loops = get_required_loops(es_connection, index_name, size)
    for offset in range(loops):
        doc = download_all_with_offset_and_key(es_connection, index_name, offset * size, size, search_item)
        elastic_docs.append(doc["hits"]["hits"])

    return elastic_docs


coun = get_index_size(es_connect, ['time-based-alerts-2020-01-2'])
print(coun)
