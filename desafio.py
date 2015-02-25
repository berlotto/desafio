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
        return novos

    def exist_than_update(self, registro):
        #Se existe o registro no elasticsearch, entao
        # atualiza e retorna True, senão retorna False
        pass

    def insert(self, registro):
        pass

    def syncronize_new_data(self, registros):
        for registro in registros:
            # Retorna false caso não exista
            if not self.exist_than_update(registro):
                self.insert(registro)


class CassandraBase(object):

    def __init__(self):
        self.session = self.connect()
        self.last_id = 0

    def connect(self):
        cluster = Cluster()
        session = cluster.connect(CASSANDRA_DB)
        return session

    def new_data(self):
        s = self.session
        for tabela in data.cassandra:
            print "Verificando %s" % tabela
            
        #verifica registros maiores que LAST_ID
        novos = None  # Aqui consulta no cassandra
        return novos

    def exist_than_update(self, registro):
        #Se existe o registro no cassandra, entao
        # atualiza e retorna True, senão retorna False
        pass

    def insert(self, registro):
        pass

    def syncronize_new_data(self, registros):
        for registro in registros:
            # Retorna false caso não exista
            if not self.exist_than_update(registro):
                self.insert(registro)


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

        c_new_data = c.new_data()
        e_new_data = e.new_data()
        if c_new_data:
            e.syncronize_new_data(c_new_data)
        if e_new_data:
            c.syncronize_new_data(e_new_data)

        sleep(PERIODICIDADE * 60)


if __name__ == '__main__':
    #daemonize()
    syncronize()
