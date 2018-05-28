#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ConfigParser
import elasticsearch
import logging
import datetime
from Collector import Collector
from elasticsearch import Elasticsearch


class Updater:
    es_client = None
    logger = None

    def __init__(self, config, logger):
        self.logger = logger
        self.es_client = Elasticsearch(
            [config.get('elasticsearch', 'ES_HOST')],
            http_auth=(config.get('elasticsearch', 'ES_USERNAME'), config.get('elasticsearch', 'ES_PASSWORD')),
            scheme="https",
            port=443,
        )
        self.es_create_index()

    def es_create_index(self):
        try:
            self.es_client.indices.create(
                index='burner',
                body={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    },
                    'mappings': {
                        'user': {
                            'properties': {
                                'user': {'index': 'not_analyzed', 'type': 'keyword'},
                                'total_spent': {'index': 'not_analyzed', "type": "float"}
                            }
                        }
                    },
                    'dynamic': 'strict'
                }
            )
        except elasticsearch.exceptions.RequestError:
            print "index already exists"

    def store_users_expenses(self, users):
        self.logger.info('Store collected data: {}'.format(users))
        for user in users:
            res = self.es_client.search(
                index='burner',
                doc_type='user',
                body={
                    '_source': ['_id', 'total_spent'],
                    'query': {
                        'match': {
                            'user': user.get('user')
                        }
                    }
                }
            )
            doc_id = [d['_id'] for d in res['hits']['hits']]
            doc_total_spent = [d['_source']['total_spent'] for d in res['hits']['hits']]

            if not res['hits']['hits']:
                print("Creating user {}".format(user.get('user')))
                res = self.es_client.index(
                    index='burner',
                    doc_type='user',
                    body=user,
                    refresh=True
                )
            else:
                print("Updating user {}, stored value {}".format(user.get('user'), doc_total_spent))
                res = self.es_client.update(
                    index='burner',
                    doc_type='user',
                    id=doc_id[0],
                    body={'doc': {"total_spent": user.get('total_spent') + doc_total_spent[0]}},
                    refresh=True
                )
            print(res['result'])


if __name__ == "__main__":

    #  Add logger for application
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(logging.INFO)

    handler = logging.FileHandler(
        filename='burner_%s.log' % datetime.datetime.now().strftime('%Y%m%d'),
        mode='a'
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    app_logger.addHandler(handler)

    config = ConfigParser.RawConfigParser()
    config.read('config.cfg')

    collector = Collector.Collector(config, app_logger)

    updater = Updater(config, app_logger)
    updater.store_users_expenses(
        collector.get_users_expenses()
    )
