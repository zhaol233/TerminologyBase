# -*- coding: utf-8 -*-
# @Time    : 2022/4/26 15:44
# @Author  : zhaoliang
# @Description: TODO

import copy
import pandas as pd
from collections import defaultdict
import numpy as np
import os
import re

from util import generate_id_with_origin_code, add_node, add_data, add_relation, generate_id
from util import _node, _relation, log, db_code

node_seq_id = 1  # node
rel_seq_id = 1  # relation

node = copy.deepcopy(_node)
alias_node = copy.deepcopy(_node)
relation = copy.deepcopy(_relation)


def get_properties(owl_path, entity):
    f = open(owl_path, 'r', encoding='UTF-8')
    lines = f.readlines()
    conceptual_Entity = {"node_id": [], "node_name": [], "tag": [], "source": [], "original_code": [], "definition": []}
    flag = False
    pattern = re.compile(r'[>](.*?)[<]', re.S)  # 最小匹配
    p97 = False

    for line in lines:
        if "<NHC0>" in line and ('A' in line or 'P' in line or ">NHC0<" in line):
            flag = True
            conceptual_Entity['original_code'].append(re.findall(pattern, line)[0])
            conceptual_Entity['source'].append("NCI")
            conceptual_Entity["tag"].append("NCI|qualifier")
            global node_seq_id
            node_id = generate_id("D", db_code['NCI'], node_seq_id)
            node_seq_id += 1
            conceptual_Entity["node_id"].append(node_id)
            p97 = False
            continue
        if flag:
            if "<P97>" in line:
                conceptual_Entity["definition"].append(re.findall(pattern, line)[0])
                p97 = True
                continue
            if "<P325>" in line:
                conceptual_Entity["definition"].append(re.findall(pattern, line)[0])
                p97 = True
                continue
            if "<P108>" in line:
                node_name = re.findall(pattern, line)[0]
                if '-' in node_name:
                    node_name = node_name.replace('-', '_')
                if ' ' in node_name:
                    node_name = node_name.replace(' ', '_')
                conceptual_Entity["node_name"].append(node_name)
                continue
        if "/owl:AnnotationProperty" in line:
            if not p97:
                # log.info(f"{conceptual_Entity['node_name'][-1]} node no definition")
                conceptual_Entity["definition"].append(np.nan)
            flag = False
            continue
    result_path = f"./results/NCI/qualifier"

    os.makedirs(result_path, exist_ok=True)
    res = pd.DataFrame(conceptual_Entity)
    res.to_csv(result_path + "./concept.csv", index=False)
    return dict(zip(conceptual_Entity["original_code"], conceptual_Entity["node_name"]))


def get_relation_concept(obo_path, entity, qualifier):
    f = open(obo_path, 'r', encoding='UTF-8')
    lines = f.readlines()
    conceptual_Entity = {"node_id": [], "node_name": [], "tag": [], "source": [], "original_code": [],
                         "domain": [], "range": []}
    conceptual_property = defaultdict(list)
    conceptual_property_flag = defaultdict(bool)
    for i in qualifier.keys():
        conceptual_property[i] = []
        conceptual_property_flag[i] = True
    domain_flag = True
    range_flag = True
    start = False
    for line in lines:
        if line.startswith("[Typedef]"):
            start = True
        if start:
            if line.startswith('id:'):
                id = line.split(": ")[1].strip()
                conceptual_Entity["original_code"].append(id)
                global node_seq_id
                node_id = generate_id("D", db_code["NCI"], node_seq_id)
                node_seq_id += 1
                conceptual_Entity["node_id"].append(node_id)
                conceptual_Entity['tag'].append("NCI|relation_tag")
                conceptual_Entity['source'].append('NCI')
                continue
            if line.startswith("name:"):
                name = line.split("e: ")[1].strip()
                conceptual_Entity["node_name"].append(name)
                continue
            if line.startswith("property_value:"):
                tmp = line.split("e: ")[1]
                key = tmp.split(' ')[0]
                value = tmp.split('\"')[1]
                if '-' in value:
                    value = value.replace('-', '_')
                if ' ' in value:
                    value = value.replace(' ', '_')
                conceptual_property[key].append(value)
                conceptual_property_flag[key] = True
                continue
            if line.startswith("domain:"):
                domain = line.split(': ')[1].split("!")[0].strip()
                domain_node_id = generate_id_with_origin_code("C", db_code["NCI"], domain)
                conceptual_Entity["domain"].append(domain_node_id)
                domain_flag = True
                continue
            if line.startswith("range:"):
                range = line.split(': ')[1].split("!")[0].strip()
                range_node_id = generate_id_with_origin_code("C", db_code["NCI"], range)
                conceptual_Entity["range"].append(range_node_id)
                range_flag = True
                continue
            if line.startswith('[Typedef]'):
                if not range_flag:
                    conceptual_Entity["range"].append(np.nan)
                range_flag = False
                if not domain_flag:
                    conceptual_Entity["domain"].append(np.nan)
                domain_flag = False
                for key in conceptual_property_flag.keys():
                    if not conceptual_property_flag[key]:
                        conceptual_property[key].append(np.nan)
                    conceptual_property_flag[key] = False
                continue

    # 收尾
    for key, value in conceptual_property_flag.items():
        if not value:
            conceptual_property[key].append(np.nan)
    res = pd.concat([pd.DataFrame(conceptual_Entity), pd.DataFrame(conceptual_property)], axis=1)

    result_path = f"./results/NCI/relation_tag"

    os.makedirs(result_path, exist_ok=True)
    res.dropna(axis=1, inplace=True, how="all")

    for colname in res.columns:
        if colname in list(qualifier.keys()):
            res.rename(columns={colname: qualifier[colname]}, inplace=True)

    res.to_csv(result_path + "./concept.csv", index=False)
    return dict(zip(conceptual_Entity["original_code"], conceptual_Entity["node_name"]))


def get_term(obo_path, entity, qualifier, relation_concept):
    f = open(obo_path, 'r', encoding='UTF-8')
    lines = f.readlines()
    global relation, node, alias_node
    conceptual_property_dict = defaultdict(list)
    conceptual_property_value = defaultdict(str)
    property2relation = ['A1', 'A2', 'A3', 'A5', 'A6', 'A7', 'A8', 'A12', 'A13', 'A14', 'A17', 'A18', 'A19', 'A20',
                         'A21', 'A22', 'A25', 'A41', 'P393', 'P90']

    for i in qualifier.keys():
        conceptual_property_dict[i] = []
        conceptual_property_value[i] = ''

    global rel_seq_id, node_seq_id
    is_A = False
    status = 0

    for line in lines:
        if line.startswith("[Term]") and status == 0:
            status = 1

        if line == '\n' and status == 1:
            status = 0
            node = add_node(node, node_id, node_name, f'Concept|{entity}|NCI', 'NCI', original_id)
            conceptual_property_dict = add_data(conceptual_property_dict, list(conceptual_property_value.values()))
            for key, value in qualifier.items():
                conceptual_property_value[key] = ''
            if not is_A:
                global no_is_a_node
                no_is_a_node = add_node(no_is_a_node, node_id, node_name, f'Concept|{entity}|NCI', 'NCI', original_id)
                # print(node_name)
            is_A = False
            continue

        if line.startswith('id:') and status == 1:
            original_id = line.split(": ")[1].strip()
            node_id = generate_id_with_origin_code("C", db_code["NCI"], original_id)
            continue

        if line.startswith("name:") and status == 1:
            node_name = line.split("e: ")[1].strip().replace('\\"', ' ')
            continue

        if line.startswith("property_value:") and status == 1:
            tmp = line.split("ue: ")[1].replace('\\"', ' ')
            key = tmp.split(' ')[0]

            try:
                value = tmp.split('\"')[1].strip()
            except IndexError:
                value = tmp.split(' ')[1]

            if key == 'P90':
                if value != node_name:
                    alias_node_id = generate_id("S", db_code["NCI"], node_seq_id)
                    node_seq_id += 1
                    alias_node = add_node(alias_node, alias_node_id, value, f'Synonym|{entity}|NCI', 'NCI', '')

                    relation_id = generate_id("R", db_code["NCI"], rel_seq_id)
                    rel_seq_id += 1
                    relation = add_relation(relation, relation_id, alias_node_id, node_id, 'alias', 'NCI', '')
                    continue
                else:
                    continue

            if key in property2relation:
                # print(key,value)
                relationed_node_id = generate_id_with_origin_code("C", db_code['NCI'], value)
                relation_id = generate_id("R", db_code["NCI"], rel_seq_id)
                rel_seq_id += 1
                relation = add_relation(relation, relation_id, node_id, relationed_node_id, qualifier[key], 'NCI', '')
                continue

            if len(conceptual_property_value[key]) != 0:
                last = conceptual_property_value[key]
                conceptual_property_value[key] = last + '|' + value
            else:
                conceptual_property_value[key] = value
            continue

        if line.startswith("relationship:") and status == 1:
            tmp = line.split("p: ")[1]
            relation_key = tmp.split(' ')[0].strip()
            relation_node_original_id = tmp.split(' ')[1].strip('\"').strip()
            relationed_node_id = generate_id_with_origin_code("C", db_code['NCI'], relation_node_original_id.strip())
            relation_id = generate_id("R", db_code["NCI"], rel_seq_id)
            rel_seq_id += 1
            relation = add_relation(relation, relation_id, node_id, relationed_node_id, relation_concept[relation_key],
                                    'NCI', '')
            continue

        if line.startswith("intersection_of: ") and status == 1:
            tmp = line.split("f: ")[1]
            relation_key = tmp.split(' ')[0].strip()
            if relation_key.startswith('C'):
                is_A = True
                relationed_node_id = generate_id_with_origin_code("C", db_code['NCI'], relation_key)
                relation_id = generate_id("R", db_code["NCI"], rel_seq_id)
                rel_seq_id += 1
                relation = add_relation(relation, relation_id, node_id, relationed_node_id,
                                        'is_A', 'NCI', '')
            else:
                relation_node_original_id = tmp.split(' ')[1].strip()
                relationed_node_id = generate_id_with_origin_code("C", db_code['NCI'], relation_node_original_id)
                relation_id = generate_id("R", db_code["NCI"], rel_seq_id)
                rel_seq_id += 1
                relation = add_relation(relation, relation_id, node_id, relationed_node_id,
                                        relation_concept[relation_key], 'NCI', '')
            continue

        if line.startswith("is_a: ") and status == 1:
            is_A = True
            relation_node_original_id = line.split('a: ')[1].split('!')[0].strip()
            relationed_node_id = generate_id_with_origin_code("C", db_code['NCI'], relation_node_original_id)
            relation_id = generate_id("R", db_code["NCI"], rel_seq_id)
            rel_seq_id += 1
            relation = add_relation(relation, relation_id, node_id, relationed_node_id, 'is_A', 'NCI', '')
            continue

        if line.startswith("[Typedef]"):
            break

    property_df = pd.DataFrame(conceptual_property_dict)
    property_df.drop(columns=property2relation + ['NHC0', 'P316'], inplace=True)
    property_df.replace(to_replace='', value=np.nan, inplace=True)
    property_df.dropna(axis=1, inplace=True, how="all")

    columns = list(property_df.columns)
    new_columns = [qualifier[i] for i in columns]
    property_df.columns = new_columns

    res = pd.concat([pd.DataFrame(node), property_df], axis=1)

    concept_path = f"./results/NCI/concept"
    os.makedirs(concept_path, exist_ok=True)
    res.to_csv(concept_path + "/concept.csv", index=False)

    synonym_path = f"./results/NCI/synonym"
    os.makedirs(synonym_path, exist_ok=True)
    pd.DataFrame(alias_node).to_csv(synonym_path + "/concept.csv", index=False)


def get_tag(entity):
    owl_path = f"./input/NCLt/branch/{entity}/{entity}.owl"
    obo_path = f"./input/NCLt/branch/{entity}.obo"
    id_qualifier = get_properties(owl_path, entity)
    relation_concept = get_relation_concept(obo_path, entity, id_qualifier)
    return id_qualifier, relation_concept


def get_is_a():
    global no_is_a_node, rel_seq_id, relation
    print(no_is_a_node['node_name'])
    node_list = list(no_is_a_node['original_code'])
    count = len(node_list)
    start_owl_line = [f"<owl:Class rdf:about=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#{original_code}\">"
                      for original_code in node_list]
    # mark_owl_line = [
    #     f"<rdf:Description rdf:about=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#{original_code}\"/>"
    #     for original_code in node_list]
    owl_line_original_code_dict = dict(zip(start_owl_line, node_list))
    owl_path = f"./input/NCLt/branch/Thesaurus.owl"
    f = open(owl_path, 'r', encoding='UTF-8')
    lines = f.readlines()
    status = 0
    for line in lines:
        if line.strip() in start_owl_line and status == 0:
            status = 1
            original_node_id = owl_line_original_code_dict[line.strip()]
        if line.strip() == '':
            status = 0
        if line.strip().startswith("<rdf:Description rdf:about=\"http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#") \
                and status == 1:
            if original_node_id in node_list:
                node_list.remove(original_node_id)
                count -= 1
            node_id = generate_id_with_origin_code("C", db_code['NCI'], original_node_id)

            relationed_original_node = line.strip().split("#")[1].split('\"')[0]
            relationed_node_id = generate_id_with_origin_code("C", db_code['NCI'], relationed_original_node)
            relation_id = generate_id("R", db_code["NCI"], rel_seq_id)
            rel_seq_id += 1
            relation = add_relation(relation, relation_id, node_id, relationed_node_id, 'is_A', 'NCI', '')

    # for value in node_list:
    #     # print(no_is_a_node.loc[no_is_a_node['node_id'] == value]['node_name'])
    #     node_id = generate_id_with_origin_code("C", db_code['NCI'], value)
    #     relation_id = generate_id("R", db_code["NCI"], rel_seq_id)
    #     rel_seq_id += 1
    #     relation = add_relation(relation, relation_id, node_id, 'C014000000000000', 'is_A', 'NCI', '')


if __name__ == "__main__":
    no_is_a_node = copy.deepcopy(_node)
    id_qualifier, relation_concept = get_tag("Abnormal_Cell")

    obo_path = f"./input/NCLt/branch/Thesaurus.obo"
    get_term(obo_path, 'Thesaurus', id_qualifier, relation_concept)
    get_is_a()

    relation_path = f"./results/NCI/relation"
    os.makedirs(relation_path, exist_ok=True)
    pd.DataFrame(relation).to_csv(relation_path + "/relation.csv", index=False)

    log.info(f"finish operate")
