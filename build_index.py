#!/usr/bin/env python
# -*- coding: utf-8 -*-


import csv
import pprint

from elasticsearch import Elasticsearch


ES_HOST = {"host": "localhost", "port": 9200}
INDEX_NAME = "titanic"
TYPE_NAME = "passenger"
ID_FIELD = "passengerid"


def generate_bulk_data(filename):
    with open(filename, 'r') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        header = [item.lower() for item in header]
        bulk_data = []
        for row in reader:
            data_dict = {}
            for i in range(len(row)):
                data_dict[header[i]] = row[i]
            op_dict = {"index": {"_index": INDEX_NAME, "_type": TYPE_NAME, "_id": data_dict[ID_FIELD]}}
            bulk_data.append(op_dict)
            bulk_data.append(data_dict)
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(bulk_data)
        return bulk_data


def build_index(bulk_data):
    es = Elasticsearch(hosts=[ES_HOST])
    if es.indices.exists(INDEX_NAME):
        print("deleting '%s' index..." % (INDEX_NAME))
        res = es.indices.delete(index=INDEX_NAME)
        print(" response: '%s'" % (res))
    request_body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    print("creating '%s' index..." % (INDEX_NAME))
    res = es.indices.create(index=INDEX_NAME, body=request_body)
    print(" response: '%s'" % (res))
    # bulk index the data
    print("bulk indexing...")
    res = es.bulk(index=INDEX_NAME, body=bulk_data, refresh=True)
    # sanity check
    res = es.search(index=INDEX_NAME, size=2, body={"query": {"match_all": {}}})
    print("results:")
    for hit in res['hits']['hits']:
        print(hit["_source"])


def main():
    bulk_data = generate_bulk_data('test.csv')
    build_index(bulk_data)


if __name__ == '__main__':
    main()
