import csv, json
import numpy as np
import pandas
from elasticsearch import Elasticsearch

# create a client instance of the library
elastic_client = Elasticsearch([{'host': 'HOST_ALB', 'port': 80}])
if elastic_client.ping():
    print('Yay Connect')
else:
    print('Awww it could not connect!')

# make an API call to the Elasticsearch cluster to get documents
print("Going to download data")

index_name = 'time-based-alerts-2020-01-2'

result = elastic_client.search(index=index_name, doc_type="_doc",
                               body={'size': 10000, 'query': {"term": {"KEY": "NO_IP_ASSIGNED"}}})
print("Data downloaded.. processing started..")
elastic_docs = result["hits"]["hits"]
source_data = []
elastic_dataframe = pandas.DataFrame()
for num, doc in enumerate(elastic_docs):
    # get _source data dict from document
    doc_data = pandas.Series(doc["_source"], name=doc["_id"])
    elastic_dataframe = elastic_dataframe.append(doc_data)


minimalized_df = elastic_dataframe[['OCCURED_TIME', 'KEY', 'MAC']]
print(minimalized_df)


print('Processing completed.. Saving as CSV')
elastic_dataframe.to_csv('no_ip_1_1.csv', sep=",")
