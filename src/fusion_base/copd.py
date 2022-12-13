# -*- coding: utf-8 -*-
# @Time    : 2022/9/9 11:28
# @Author  : zhaoliang
# @Description: TODO
import os

import pandas as pd

from term.src.utils.util import generate_id, db_code, add_data


def nci():
    target = ['C014000000003199', 'C014000000125756', 'C014000000122007', 'C014000000122006', 'C014000000161464',
              'C014000000119100', 'C014000000119099']
    concept = pd.read_csv("../../results/NCI/concept/concept.csv")
    synonym = pd.read_csv("../../results/NCI/synonym/concept.csv")

    relation = pd.read_csv("../../results/NCI/relation/relation.csv")

    direct_relation = relation.loc[relation['node_id'].isin(target)]
    direct_relation = pd.concat([relation.loc[relation['relationed_node_id'].isin(target)], direct_relation], axis=0)

    print("direct_relation", direct_relation.shape)  # (30)

    direct_node = set(list(direct_relation['node_id']) + list(direct_relation['relationed_node_id']))
    print("direct_node", len(direct_node))

    is_A_df = relation.loc[relation['relation_tag'] == 'is_A']

    parent_l = list(direct_node)
    child_l = list(direct_node)
    while len(child_l):
        parent_df = is_A_df[is_A_df['node_id'].isin(child_l)]
        direct_relation = pd.concat([direct_relation, parent_df], axis=0)
        child_l = list(parent_df['relationed_node_id'])
        parent_l += child_l
    parent_l = list(set(parent_l))

    copd_concept = concept.loc[concept['node_id'].isin(parent_l)]
    copd_synonym = synonym.loc[synonym['node_id'].isin(parent_l)]

    concept_path = "../../results/merge_db/copd/NCI/concept"
    os.makedirs(concept_path, exist_ok=True)
    pd.DataFrame(copd_concept).to_csv(concept_path + "/concept.csv", index=False)

    synonym_path = "../../results/merge_db/copd/NCI/synonym"
    os.makedirs(synonym_path, exist_ok=True)
    pd.DataFrame(copd_synonym).to_csv(synonym_path + f"/concept.csv", index=False)

    direct_relation.drop_duplicates(subset=['relation_id'], inplace=True)
    relation_path = "../../results/merge_db/copd/NCI/relation"
    os.makedirs(relation_path, exist_ok=True)
    pd.DataFrame(direct_relation).to_csv(relation_path + f"/relation.csv", index=False)


def hpo():
    target = 'C012000000006510'
    concept = pd.read_csv("../../results/hpo/concept/concept.csv")
    synonym = pd.read_csv("../../results/hpo/synonym/concept.csv")

    relation = pd.read_csv("../../results/hpo/relation/relation.csv")
    direct_relation = relation.loc[relation['node_id'] == target]
    direct_relation = pd.concat([relation.loc[relation['relationed_node_id'] == target], direct_relation], axis=0)
    direct_node = set(list(direct_relation['node_id']) + list(direct_relation['relationed_node_id']))

    is_A_df = relation.loc[relation['relation_tag'] == 'is_A']

    parent_l = list(direct_node)
    child_l = list(direct_node)
    while len(child_l):
        parent_df = is_A_df[is_A_df['node_id'].isin(child_l)]
        direct_relation = pd.concat([direct_relation, parent_df], axis=0)
        child_l = list(parent_df['relationed_node_id'])
        parent_l += child_l
    parent_l = list(set(parent_l))

    copd_concept = concept.loc[concept['node_id'].isin(parent_l)]
    copd_synonym = synonym.loc[synonym['node_id'].isin(parent_l)]

    concept_path = "../../results/merge_db/copd/hpo/concept"
    os.makedirs(concept_path, exist_ok=True)
    pd.DataFrame(copd_concept).to_csv(concept_path + "/concept.csv", index=False)

    synonym_path = "../../results/merge_db/copd/hpo/synonym"
    os.makedirs(synonym_path, exist_ok=True)
    pd.DataFrame(copd_synonym).to_csv(synonym_path + f"/concept.csv", index=False)

    direct_relation.drop_duplicates(subset=['relation_id'], inplace=True)
    relation_path = "../../results/merge_db/copd/hpo/relation"
    os.makedirs(relation_path, exist_ok=True)
    pd.DataFrame(direct_relation).to_csv(relation_path + f"/relation.csv", index=False)


def merge():
    # NCI
    relation = {"relation_id": [], "node_id": [], 'node_source': [], "relationed_node_id": [],
                "relationed_node_source": [], "relation_tag": [], "source": [], "original_code": []}

    rel_sep_id = 1

    # HPO
    relation_id = generate_id('R', db_code['copd'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C012000000000118', 'HPO', '2', "COPD Base", 'is_A', "COPD Base", ''])

    # NCI gene
    relation_id = generate_id('R', db_code['copd'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000020194', 'NCI', '13', "COPD Base", 'is_A', "COPD Base", ''])

    # NCI activity
    relation_id = generate_id('R', db_code['copd'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000043431', 'NCI', '6', "COPD Base", 'is_A', "COPD Base", ''])

    # NCI disease
    relation_id = generate_id('R', db_code['copd'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000007057', 'NCI', '1', "COPD Base", 'is_A', "COPD Base", ''])

    # NCI property or attribute
    relation_id = generate_id('R', db_code['copd'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000020189', 'NCI', '19', "COPD Base", 'is_A', "COPD Base", ''])

    # NCI gene product
    relation_id = generate_id('R', db_code['copd'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000026548', 'NCI', '12', "COPD Base", 'is_A', "COPD Base", ''])

    relation_path = "../../results/merge_db/copd/relation"
    os.makedirs(relation_path, exist_ok=True)
    pd.DataFrame(relation).to_csv(relation_path + "/relation.csv", index=False)


if __name__ == "__main__":
    # nci()
    # hpo()
    merge()
