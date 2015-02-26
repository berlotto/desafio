# -*- encoding: utf-8 -*-
#
# Desafio: Sincronização de dados Cassandra-Elasticsearch
# Author: Sergio Berlotto <sergio.berlotto@gmail.com>
#

import os
import sys
import data_config as data
from time import sleep

PERIODICIDADE = 1  # Em minutos
CASSANDRA_CONNECTION = ""
CASSANDRA_DB = ""
ELASTIC_CONNECTION = ""
ELASTIC_DB_NAME = "desafio"

from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch


class ElasticBase(object):

    def __init__(self):
        self.connect()
        self.last_id = 0

    def connect(self):
        self.es = Elasticsearch()
        #Create the database
        self.es.indices.create(index=ELASTIC_DB_NAME, ignore=400)
        return es

    def new_data(self):
        #verifica registos maiores que LAST_ID
        novos = None
        for dado in data.elasticsearch:
            res = self.es.search(
                index=ELASTIC_DB_NAME,
                doc_type=dado['table_name'],
                body={
                    'query': {
                        'filtered': {
                            'filter': {
                                'range': {
                                    dado['id_field']: {'gt': self.last_id}
                                }
                            }
                        }
                    }
                }
            )
            novos = res['hits']['hits']
        return novos

    def exist_than_update(self, tabela, registro):
        #Se existe o registro no elasticsearch, entao
        # atualiza e retorna True, senão retorna False
        reg = es.get(
            index=ELASTIC_DB_NAME,
            doc_type=tabela['table_name'],
            id=registro['_id']
        )
        if reg:
            es.update(
                index=ELASTIC_DB_NAME,
                doc_type=tabela['table_name'],
                id=registro['_id'],
                body={'doc': registro}
            )
            return True
        else:
            return False

    def insert(self, tabela, registro):
        self.es.index(
            index=ELASTIC_DB_NAME,
            doc_type=tabela['table_name'],
            body=registro
        )

    def syncronize_new_data(self, registros):
        tabela, registro = tabela_registro
        # Retorna false caso não exista
        if not self.exist_than_update(tabela, registro):
            self.insert(tabela, registro)


class CassandraBase(object):

    def __init__(self):
        self.session = self.connect()
        self.last_id = 0

    def connect(self):
        cluster = Cluster()
        session = cluster.connect(CASSANDRA_DB)
        return session

    def new_data(self):
        # Coomo gerador, retorna cada registro e sua tabela
        s = self.session
        for tabela in data.cassandra:
            print "Verificando %s" % tabela['table_name']
            novos = s.execute(
                'SELECT * FROM %s where %s > %s' % (
                    tabela['table_name'],
                    tabela['id_field'],
                    self.last_id
                )
            )
            for novo in novos:
                yield (tabela, novo)

    def exist_than_update(self, tabela, registro):
        #Se existe o registro no cassandra, entao
        # atualiza e retorna True, senão retorna False
        s = self.session
        one = s.execute(
            "SELECT * FROM %s WHERE %s = '%s'" % (
                tabela['table_name'],
                tabela['id_field'],
                registro[tabela['id_field']]
            )
        )
        if one:
            # Update
            return True
        else:
            return False

    def insert(self, tabela, registro={}):
        if not registro:
            return
        # registro é um dicionário de dados
        SQL = "INSERT INTO %s " % tabela['table_name']
        fields = []
        fields_str = ""
        for key in registro.keys():
            SQL += "%s, " % key
            if type(registro[key] == int):
                fields_str += "%d, "
            else:
                fields_str += "%s, "
            fields.append(registro[key])
        SQL += ") VALUES ( %s )" % fields_str

        session.execute(SQL, fields)

    def syncronize_new_data(self, tabela_registro):
        tabela, registro = tabela_registro
        # Retorna false caso não exista
        if not self.exist_than_update(tabela, registro):
            self.insert(tabela, registro)


def daemonize():
    # Cria novo processo
    pid = os.fork()
    if pid != 0:
        sys.exit()  # Termina o processo pai
    # Cria nova sessão
    os.setsid()
    # Muda a máscara de arquivos
    os.umask(0)
    # Muda o diretório atual
    os.chdir(".")


# PRINCIPAL
def syncronize():

    c = CassandraBase()
    e = ElasticBase()

    while True:

        for data in c.new_data():
            e.syncronize_new_data(data)

        for data in e.new_data():
            c.syncronize_new_data(data)

        sleep(PERIODICIDADE * 60)


if __name__ == '__main__':
    #daemonize()
    syncronize()
