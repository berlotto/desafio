# -*- encoding: utf-8 -*-
#
# Desafio: Sincronização de dados Cassandra-Elasticsearch
# Author: Sergio Berlotto <sergio.berlotto@gmail.com>
# git: https://github.com/berlotto/desafio
#

from cassandra.cluster import Cluster
from cassandra import AlreadyExists
from config import *

cluster = Cluster()
session = cluster.connect()

try:
    session.execute("""
        CREATE KEYSPACE %s
        WITH replication = {
            'class':'SimpleStrategy',
            'replication_factor':3
        };
    """ % CASSANDRA_DB)
except AlreadyExists:
    print "Keyspace alread exists"

session.set_keyspace(CASSANDRA_DB)

try:
    session.execute("""
        CREATE TABLE %s.users (
            id uuid PRIMARY KEY,
            name text,
            birth_year int,
            email text,
            active boolean,
        );
    """ % CASSANDRA_DB)
except AlreadyExists:
    print "Table users alread exists"

try:
    session.execute("""
        CREATE TABLE %s.books (
            id uuid PRIMARY KEY,
            name text,
            description text,
            section text,
            tags set<text>,
        );
    """ % CASSANDRA_DB)
except AlreadyExists:
    print "Table books alread exists"

print "Cassandra database created"
