# -*- encoding: utf-8 -*-
#
# Desafio: Sincronização de dados Cassandra-Elasticsearch
# Author: Sergio Berlotto <sergio.berlotto@gmail.com>
# git: https://github.com/berlotto/desafio
#

import os
import sys
from time import sleep

from config import * 

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
        return self.es

    def new_data(self):
        #verifica registos maiores que LAST_ID
        print "E.new_data"
        novos = None
        for dado in SYNCDATA:
            print "Verificando E %s" % dado['table_name']
            if self.last_id:
                body = {
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
            else:
                body = None
            res = self.es.search(
                index=ELASTIC_DB_NAME,
                doc_type=dado['table_name'],
                body=body
            )
            print "Retorno E", novos
            novos = res['hits']['hits']
            for novo in novos:
                print "Retornando E", novo
                self.last_id = novo['_source']['id']
                yield (tabela, novo['_source'])

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
        self.last_id = None

    def connect(self):
        cluster = Cluster()
        session = cluster.connect(CASSANDRA_DB)
        return session

    def new_data(self):
        # Coomo gerador, retorna cada registro e sua tabela
        print "C.new_Data"
        s = self.session
        for tabela in SYNCDATA:
            print "Verificando %s" % tabela['table_name']
            if self.last_id:
                novos = s.execute(
                    'SELECT * FROM %s where %s > %s' % (
                        tabela['table_name'],
                        tabela['id_field'],
                        self.last_id
                    )
                )
            else:
                novos = s.execute(
                    'SELECT * FROM %s ' % (
                        tabela['table_name']
                    )
                )
            print "Retornando registros..."
            for novo in novos:
                print "novo"
                self.last_id = novo['id']
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
            SQL = "UPDATE %s SET " % tabela['table_name']

            for key in registro.keys():
                if type(registro[key] == int):
                    SQL += "%s = %s, " % (key, registro['key'])
                else:
                    SQL += "%s = '%s', " % (key, registro['key'])

            SQL += "WHERE  %s = %s" % (
                tabela['id_field'],
                registro[tabela['id_field']]
            )

            session.execute(SQL)
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
            print "Redcebido C", data
            e.syncronize_new_data(data)

        for data in e.new_data():
            print "Recebido E", data
            c.syncronize_new_data(data)

        print "Waiting..."
        sleep(FREQUENCY * 60)


if __name__ == '__main__':
    #daemonize()
    syncronize()
