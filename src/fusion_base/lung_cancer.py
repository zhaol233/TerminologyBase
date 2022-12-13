# -*- coding: utf-8 -*-
# @Time    : 2022/11/25 20:17
# @Author  : zhaol233
# @Description: TODO

import pandas as pd
from term.src.utils.util import generate_id_with_origin_code, add_node, add_data, add_relation, generate_id
from term.src.utils.util import base_node, base_relation, log, db_code, root_path
import os

import copy


def NCI():
    # C014000000004878  --  Lung Carcinoma

    nci_concept_df = pd.read_csv('../../results/NCI/concept/concept.csv')
    nci_synonym_df = pd.read_csv('../../results/NCI/synonym/concept.csv')
    nci_relation_df = pd.read_csv('../../results/NCI/relation/relation.csv', header=0)
    all_is_a_relation = nci_relation_df.loc[nci_relation_df['relation_tag'] == "is_A", :]

    to_relation = nci_relation_df.loc[nci_relation_df['relationed_node_id'] == "C014000000004878"]
    from_relation = nci_relation_df.loc[nci_relation_df['node_id'] == "C014000000004878"]

    relation = pd.concat([to_relation, from_relation])
    relation.drop_duplicates(subset=['relation_id'], inplace=True)
    node_l = list(relation['node_id']) + list(relation['relationed_node_id'])

    node_l = set(node_l)

    df_up_is_a = all_is_a_relation[all_is_a_relation['node_id'].isin(node_l)]
    while df_up_is_a.shape[0] != 0:
        relation = pd.concat([relation, df_up_is_a])
        up_node = set(df_up_is_a['relationed_node_id'])
        df_up_is_a = all_is_a_relation[all_is_a_relation['node_id'].isin(up_node)]

    relation.drop_duplicates(subset=['relation_id'], inplace=True)
    node_l = list(relation['node_id']) + list(relation['relationed_node_id'])
    concept_node = nci_concept_df[nci_concept_df['node_id'].isin(node_l)]
    synonym_node = nci_synonym_df[nci_synonym_df['node_id'].isin(node_l)]

    concept_path = root_path + "/results/merge_db/lung/NCI/concept"
    os.makedirs(concept_path, exist_ok=True)
    concept_node.to_csv(concept_path + "/concept.csv", index=False)

    synonym_path = root_path + "/results/merge_db/lung/NCI/synonym"
    os.makedirs(synonym_path, exist_ok=True)
    synonym_node.to_csv(synonym_path + "/concept.csv", index=False)

    relation_path = root_path + "/results/merge_db/lung/NCI/relation"
    os.makedirs(relation_path, exist_ok=True)
    relation.to_csv(relation_path + "/relation.csv", index=False)


def uniprot():
    df_lung = pd.read_csv(root_path + "/input/UNIPROT/lung_cancer_2022.11.25-13.04.12.18.tsv", sep='\t', header=0)
    df_uniprot = pd.read_csv(root_path + "/results/uniprot/concept/concept.csv", header=0)
    df_relation = pd.read_csv(root_path + "/results/uniprot/relation/relation.csv", header=0)

    df_hgnc_concept = pd.read_csv(root_path + "/results/hgnc/concept/concept.csv", header=0)
    df_hgnc_synonym = pd.read_csv(root_path + "/results/hgnc/synonym/concept.csv", header=0)
    df_hgnc_relation = pd.read_csv(root_path + "/results/hgnc/relation/relation.csv", header=0)

    df_uniprot_concept = df_uniprot.loc[df_uniprot['original_code'].isin(df_lung['Entry'])]
    df_relation = df_relation.loc[df_relation['node_id'].isin(df_uniprot_concept['node_id'])]
    root_node = df_uniprot[df_uniprot['node_name'] == 'PROTEIN']
    df_uniprot_concept = pd.concat([df_uniprot_concept, root_node])

    concept_path = root_path + "/results/merge_db/lung/uniprot/concept"
    os.makedirs(concept_path, exist_ok=True)
    df_uniprot_concept.to_csv(concept_path + "/concept.csv", index=False)

    relation_path = root_path + "/results/merge_db/lung/uniprot/relation"
    os.makedirs(relation_path, exist_ok=True)
    df_relation.to_csv(relation_path + "/relation.csv", index=False)

    gene_name = []
    for i in list(df_lung['Gene Names'].apply(lambda x: str(x).split(' '))):
        gene_name += i
    df_hgnc_lung_concept = df_hgnc_concept.loc[df_hgnc_concept['node_name'].isin(set(gene_name))]
    df_hgnc_lung_relation = pd.concat(
        [df_hgnc_relation.loc[df_hgnc_relation['node_id'].isin(df_hgnc_lung_concept['node_id'])],
         df_hgnc_relation.loc[df_hgnc_relation['relationed_node_id'].isin(df_hgnc_lung_concept['node_id'])]
         ])
    df_hgnc_lung_relation.drop_duplicates(subset=['relation_id'], inplace=True)
    node_l = list(df_hgnc_lung_relation['node_id']) + list(df_hgnc_lung_relation['relationed_node_id'])
    df_hgnc_lung_concept = df_hgnc_concept[df_hgnc_concept['node_id'].isin(node_l)]
    df_hgnc_lung_synonym = df_hgnc_synonym[df_hgnc_synonym['node_id'].isin(node_l)]

    concept_path = root_path + "/results/merge_db/lung/hgnc/concept"
    os.makedirs(concept_path, exist_ok=True)
    df_hgnc_lung_concept.to_csv(concept_path + "/concept.csv", index=False)

    synonym_path = root_path + "/results/merge_db/lung/hgnc/synonym"
    os.makedirs(synonym_path, exist_ok=True)
    df_hgnc_lung_synonym.to_csv(synonym_path + "/concept.csv", index=False)

    relation_path = root_path + "/results/merge_db/lung/hgnc/relation"
    os.makedirs(relation_path, exist_ok=True)
    df_hgnc_lung_relation.to_csv(relation_path + "/relation.csv", index=False)


def hpo():
    # C012000000100526 Neoplasm of the lung
    hpo_concept = pd.read_csv(root_path + "/results/hpo/concept/concept.csv", header=0)
    hpo_synonym = pd.read_csv(root_path + "/results/hpo/synonym/concept.csv", header=0)
    hpo_relation = pd.read_csv(root_path + "/results/hpo/relation/relation.csv", header=0)
    all_is_a_relation = hpo_relation.loc[hpo_relation['relation_tag'] == "is_A", :]

    to_relation = hpo_relation.loc[hpo_relation['relationed_node_id'] == "C012000000100526"]
    from_relation = hpo_relation.loc[hpo_relation['node_id'] == "C014000000004878"]

    relation = pd.concat([to_relation, from_relation])
    relation.drop_duplicates(subset=['relation_id'], inplace=True)
    node_l = list(relation['node_id']) + list(relation['relationed_node_id'])

    node_l = set(node_l)

    df_up_is_a = all_is_a_relation[all_is_a_relation['node_id'].isin(node_l)]
    while df_up_is_a.shape[0] != 0:
        relation = pd.concat([relation, df_up_is_a])
        up_node = set(df_up_is_a['relationed_node_id'])
        df_up_is_a = all_is_a_relation[all_is_a_relation['node_id'].isin(up_node)]

    relation.drop_duplicates(subset=['relation_id'], inplace=True)
    node_l = list(relation['node_id']) + list(relation['relationed_node_id'])
    concept_node = hpo_concept[hpo_concept['node_id'].isin(node_l)]
    synonym_node = hpo_synonym[hpo_synonym['node_id'].isin(node_l)]

    concept_path = root_path + "/results/merge_db/lung/hpo/concept"
    os.makedirs(concept_path, exist_ok=True)
    concept_node.to_csv(concept_path + "/concept.csv", index=False)

    synonym_path = root_path + "/results/merge_db/lung/hpo/synonym"
    os.makedirs(synonym_path, exist_ok=True)
    synonym_node.to_csv(synonym_path + "/concept.csv", index=False)

    relation_path = root_path + "/results/merge_db/lung/hpo/relation"
    os.makedirs(relation_path, exist_ok=True)
    relation.to_csv(relation_path + "/relation.csv", index=False)


if __name__ == '__main__':
    # NCI()
    # uniprot()
    hpo()

