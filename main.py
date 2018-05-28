#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ConfigParser
import elasticsearch
import logging
import datetime
from Collector import Collector
from elasticsearch import Elasticsearch, RequestsHttpConnection


class Updater:
    es_client = None
    logger = None

    def __init__(self, config, logger):
        self.logger = logger
        params = {
            'hosts': "https://{}:{}@{}".format(
                config.get('elasticsearch', 'ES_USERNAME'),
                config.get('elasticsearch', 'ES_PASSWORD'),
                config.get('elasticsearch', 'ES_HOST')
            ),
            'connection_class': RequestsHttpConnection,
            'use_ssl': True,
            'verify_certs': True
        }
        self.es_client = Elasticsearch(**params)
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
                },
                request_timeout=300
            )
        except (elasticsearch.exceptions.RequestError, elasticsearch.exceptions.ConnectionTimeout) as e:
            self.logger.info('Elasticsearch expection: {}'.format(e))

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
                },
                request_timeout=300
            )
            doc_id = [d['_id'] for d in res['hits']['hits']]
            doc_total_spent = [d['_source']['total_spent'] for d in res['hits']['hits']]

            if not res['hits']['hits']:
                self.logger.info('Creating user {}'.format(user.get('user')))
                res = self.es_client.index(
                    index='burner',
                    doc_type='user',
                    body=user,
                    refresh=True,
                    request_timeout=300
                )
            else:
                self.logger.info('Updating user {}'.format(user.get('user')))
                res = self.es_client.update(
                    index='burner',
                    doc_type='user',
                    id=doc_id[0],
                    body={'doc': {"total_spent": user.get('total_spent') + doc_total_spent[0]}},
                    refresh=True,
                    request_timeout=300
                )
            self.logger.info('Elasticsearch response {}'.format(res))


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
    app_logger.info('Stared')

    config = ConfigParser.RawConfigParser()
    config.read('config.cfg')

    collector = Collector.Collector(config, app_logger)

    updater = Updater(config, app_logger)
    updater.store_users_expenses(
        collector.get_users_expenses()
    )

    app_logger.info('Completed')
    app_logger.info('')
