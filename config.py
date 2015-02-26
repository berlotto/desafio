# -*- encoding: utf-8 -*-
#
# Desafio: Sincronização de dados Cassandra-Elasticsearch
# Author: Sergio Berlotto <sergio.berlotto@gmail.com>
# git: https://github.com/berlotto/desafio
#

FREQUENCY = 1  # Em minutos

CASSANDRA_CONNECTION = ["127.0.0.1", ]
CASSANDRA_DB = "desafio"

ELASTIC_CONNECTION = ""
ELASTIC_DB_NAME = "desafio"

# Dados que serão sincronizados entre os databases.
SYNCDATA = [{
    'table_name': "users",
    'id_field': 'id',
    'sync_fields': ['name', 'birth_year', 'email', 'active']
}, {
    'table_name': "books",
    'id_field': 'id',
    'sync_fields': ['name', 'description', 'section', 'tags']
}]
