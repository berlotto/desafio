# -*- encoding: utf-8 -*-
#
# Desafio: Sincronização de dados Cassandra-Elasticsearch
# Author: Sergio Berlotto <sergio.berlotto@gmail.com>
# Configuracao dos dados a serem sincronizados
#

cassandra = [{
    'table_name': "table1",
    'id_field': 'id',
    'sync_fields': ['field2', 'field3', 'field4']
}]

elasticsearch = [{
    'table_name': "table1",
    'id_field': 'id',
    'sync_fields': ['field2', 'field3', 'field4']
}]
