# -*- coding: utf-8 -*-
# @Time    : 2022/6/10 15:26
# @Author  : zhaoliang
# @Description: TODO
import copy
import os

import pandas as pd

from py2neo import Graph

local_dir = "../results"

g = Graph("http://172.20.137.104:17475", password="covid-19")

TYPE = {
    'Concept': 'cid',
    'Description': 'did',
    'Synonym': 'sid',
    'Relation': 'rid'
}


def get_file_from_dir(path):
    true_path = local_dir + path
    relation = os.listdir(true_path)

    relation = [path + '/' + i for i in relation if i != 'test.csv']
    print(relation)
    return relation


def delete(label):
    cypher = f"match (n:{label}) delete n"
    g.run(cypher)


def delete_all():
    for label in ["HGNC", "NCI", "UniProt", "GO", "HPO"]:
        delete(label)


def create_index(label):
    cypher = f"CREATE INDEX ON :{label}(import_id)"
    g.run(cypher)


def create_node_from_csv(file_path, label, type):
    """

    Args:
        file_path: /hgnc/concept.csv
        label: Concept:HGNC:GENE、Synonym:HGNC:GENE
        type: Concept

    Returns:

    """
    print(label)
    headers = pd.read_csv(local_dir + file_path, header=0).columns
    attribution_cypher = ''
    for attribute in headers:
        attribution_cypher += f"{attribute}:coalesce(row.{attribute},''),"

    cypher = """USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM "file:%s" AS row
    create(:%s {import_id:row.node_id,%s:row.node_id, history_id:[],name:row.node_name,%s})
    """ % (file_path, label, TYPE[type], attribution_cypher[:-1])
    print(cypher)
    # g.run(cypher)


def create_node_from_csv_for17475(file_path, label, type):
    """

    Args:
        source:
        file_path: /hgnc/concept.csv
        label: Concept:HGNC:GENE、Synonym:HGNC:GENE
        type: Concept

    Returns:

    """
    print(label)
    # headers = pd.read_csv(local_dir + file_path, header=0).columns
    # attribution_cypher = ''
    # for attribute in headers:
    #     attribution_cypher += f"{attribute}:coalesce(row.{attribute},''),"

    cypher = """USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM "file:%s" AS row
    create(:%s {node_id:row.node_id,import_id:row.node_id,%s:row.node_id, history_id:[],name:row.node_name,
    original_code:row.original_code, node_name:row.node_name,source:row.source,tag:row.tag})
    """ % (file_path, label, TYPE[type])
    print(cypher)
    g.run(cypher)


def create_relation_from_csv(file_path, relation, label1, label2):
    """
    Args:
        file_path: /hgnc/is_A.csv
        relation: is_A
        label: HGNC
    """
    print(label1, relation, label2)
    if relation == 'alias':
        cypher = """
        LOAD CSV WITH HEADERS FROM "file:%s" AS row
        MATCH (s:%s{import_id: row.node_id})
        MATCH (e:%s{import_id: row.relation_node_id})
        merge (e)-[r:%s]->(s)
        ON CREATE  SET r = {
            rid:row.relation_id,source:row.source,history_id: [],relation_tag:row.relation_tag,import_id:row.relation_id,
            relationed_node_id:row.node_id, node_id:row.relation_node_id
        }
        RETURN count(r);
        """ % (file_path, label1, label2, relation)
        # print(cypher)
        # g.run(cypher)
    else:
        cypher = """
        LOAD CSV WITH HEADERS FROM "file:%s" AS row
        MATCH (s:%s{import_id: row.node_id})
        MATCH (e:%s{import_id: row.relation_node_id})
        merge (s)-[r:%s]->(e)
        ON CREATE  SET r = {
            rid:row.relation_id,source:row.source,history_id: [],relation_tag:row.relation_tag,import_id:row.relation_id,
            relationed_node_id:row.relation_node_id, node_id:row.node_id
        }
        RETURN count(r);
        """ % (file_path, label1, label2, relation)
        pass
    print(cypher)
    g.run(cypher)


def to_root(file_path, label1, label2, name):
    print(file_path, label1, label2, name)
    cypher = """
     LOAD CSV WITH HEADERS FROM "file:%s" AS row
        MATCH (s:%s{import_id: row.node_id})
        MATCH (e:%s{name: "%s"})
        merge (s)-[r:is_A]->(e)
        ON CREATE  SET r = {
            source:'Covid-19 Base',history_id: [],import_id:coalesce(row.attribute_,'')
        }
        RETURN count(r);
    """ % (file_path, label1, label2, name)
    print(cypher)
    g.run(cypher)


def add_label(file_path, raw_label, add_label):
    cypher = """
     LOAD CSV WITH HEADERS FROM "file:%s" AS row
            MATCH (n:%s{import_id: row.relation_node_id})
            set n:%s
            RETURN count(n);
    """ % (file_path, raw_label, add_label)
    print(cypher)
    g.run(cypher)


def add_related_node_label(file_path, label, relation):
    cypher = """
    LOAD CSV WITH HEADERS FROM "file:%s" AS row
    MATCH (n:NCI:Concept{import_id: row.node_id})
    match (m:%s)
    WHERE (m)-[:%s]->(n)
    set m:Covid19
    return count(m)
    """ % (file_path, label, relation)
    print(cypher)
    g.run(cypher)


def killall():
    """call dbms.listQueries() yield query, elapsedTimeMillis, queryId, username
where elapsedTimeMillis >30000
with query, collect(queryId) as q
call dbms.killQueries(q) yield queryId
return query, queryId"""


def reverse_rel():
    for data in ['NCI']:
        cypher = f"""match(n: {data}:Concept)
                    match (n)-[r:alias]->(m:{data}:Synonym)
                    merge(m)-[r1: alias]->(n)
                    on create set r1 = r
                    delete r
                    return count(r)"""
        print(cypher)
        g.run(cypher)
        # break
