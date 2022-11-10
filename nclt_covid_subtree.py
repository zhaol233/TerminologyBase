# -*- coding: utf-8 -*-
# @Time    : 2022/8/18 14:40
# @Author  : zhaoliang
# @Description: TODO
import pandas as pd
from util import generate_id_with_origin_code, add_node, add_data, add_relation, generate_id
from util import _node, _relation, log, db_code
import os

import copy

covid_19_subset = {
    "NCIt COVID-19 Terminology": "C173381",
}

term_df = copy.deepcopy(_node)

nci_concept_df = pd.read_csv('./results/NCI/concept/concept.csv')
nci_synonym_df = pd.read_csv('./results/NCI/synonym/concept.csv')
nci_relation_df = pd.read_csv('./results/NCI/relation/relation.csv')

direct_relation_node = nci_relation_df.loc[nci_relation_df['relationed_node_id'] == "C014000000173381"]
covid_is_a_node = direct_relation_node.loc[direct_relation_node['relation_tag'] == "Concept_In_Subset"]

direct_node_l = list(direct_relation_node['node_id'])

second_from_node = nci_relation_df[nci_relation_df['relationed_node_id'].isin(direct_node_l)]
second_to_node = nci_relation_df[nci_relation_df['node_id'].isin(direct_node_l)]

relation = pd.concat([second_from_node, second_to_node])

node_l = list(relation['node_id']) + list(relation['relationed_node_id']) + list(covid_is_a_node['node_id'])
node_l = list(set(node_l))

print(len(node_l))

concept_node = nci_concept_df[nci_concept_df['node_id'].isin(node_l)]
synonym_node = nci_synonym_df[nci_synonym_df['node_id'].isin(node_l)]
print(concept_node.shape, synonym_node.shape)

concept_path = "./results/merge_db/covid/concept"
os.makedirs(concept_path, exist_ok=True)
concept_node.to_csv(concept_path + "/concept.csv", index=False)

synonym_path = "./results/merge_db/covid/synonym"
os.makedirs(synonym_path, exist_ok=True)
synonym_node.to_csv(synonym_path + "/concept.csv", index=False)

relation_path = "./results/merge_db/covid/relation"
os.makedirs(relation_path, exist_ok=True)
relation.to_csv(relation_path + "/relation.csv", index=False)

