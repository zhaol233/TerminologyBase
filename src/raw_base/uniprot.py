# -*- coding: utf-8 -*-
# @Time    : 2022/4/26 15:44
# @Author  : zhaoliang
# @Description: TODO

import copy
import pandas as pd
import os
from util import add_data, generate_id, add_relation
from util import _node, _relation, log, db_code

uniprot_path = "../../input/UNIPROT/uniprot-filtered-organism__Homo+sapiens+(Human)+[9606]_.xlsx"


def uniprot():
    log.info("-----------UniProt---------------")
    data = pd.read_excel(io=uniprot_path)
    data.fillna("", inplace=True)

    node = copy.deepcopy(_node)
    node.update({"function": [], 'Proteomes': []})
    relation = copy.deepcopy(_relation)

    node_name = list(data['Protein names'])
    node_id = [generate_id('C', db_code["UniProt"], i) for i in range(1, data.shape[0] + 1)]
    original_code = list(data['Entry'])
    function = list(data['Function [CC]'])
    Proteomes = list(data['Proteomes'])
    node['node_id'] = node_id
    node['node_name'] = node_name
    node['original_code'] = original_code
    node['tag'] = ['Concept|Protein|UniProt' for _ in node_id]
    node['source'] = ['UniProt' for _ in node_id]
    node['function'] = function
    node['Proteomes'] = Proteomes

    relation['relation_id'] = [generate_id('R', db_code["UniProt"], i) for i in range(len(node_id))]
    relation['node_id'] = node_id
    relation['relationed_node_id'] = ['C01100000000000000' for _ in range(len(node_id))]
    relation['relation_tag'] = ['is_A' for _ in range(len(node_id))]
    relation['source'] = ['UniProt' for _ in range(len(node_id))]
    relation['original_code'] = ['' for _ in range(len(node_id))]

    # print(len(relation['node_id']), len(node_id))
    # for key, value in relation.items():
    #     print(key, len(relation[key]))
    # print(list(relation.keys()))

    relation_path = "../../results/uniprot/relation"
    os.makedirs(relation_path, exist_ok=True)
    pd.DataFrame(relation).to_csv(relation_path + "/relation.csv", index=False)

    concept_path = "../../results/uniprot/concept"
    os.makedirs(concept_path, exist_ok=True)
    node = add_data(node,
                    ['C01100000000000000', 'PROTEIN', 'Concept|UniProt', "UniProt", '', 'virtual root node', ''])

    df_protein = pd.DataFrame(node)
    df_protein.to_csv(concept_path + "/concept.csv", index=False)

    # gene_coding_protein
    gene_coding_protein = pd.read_csv("../../results/hgnc/gene_coding_protein/relation_raw.csv")
    gene_coding_protein_df = copy.deepcopy(_relation)

    df_protein.set_index('original_code', inplace=True)
    for idx, row in gene_coding_protein.iterrows():
        protein_name = row['relation_node_name']
        if protein_name in df_protein.index:
            node_id = row.loc['node_id']
            relation_node_id = df_protein.loc[protein_name, 'node_id']
            gene_coding_protein_df = add_relation(gene_coding_protein_df, '', node_id, relation_node_id,
                                                  'Gene_Coding_Protein', 'HGNC', '')
    concept_path = "../../results/mapping/gene_coding_protein"
    os.makedirs(concept_path, exist_ok=True)
    pd.DataFrame(gene_coding_protein_df).to_csv(concept_path + "/relation.csv", index=False)

    # protein to go
    protein2go = pd.read_csv("../../results/go/protein2go_relation/relation_raw.csv")
    protein2go_df = copy.deepcopy(_relation)

    for idx, row in protein2go.iterrows():
        protein_name = row['node_id']
        if protein_name in df_protein.index:
            relation_node_id = row.loc['relationed_node_id']
            relation_tag = row.loc['relation_tag']
            node_id = df_protein.loc[protein_name, 'node_id']
            protein2go_df = add_relation(protein2go_df, '', node_id, relation_node_id,
                                         relation_tag, 'GO', '')
    concept_path = "../../results/mapping/protein2go"
    os.makedirs(concept_path, exist_ok=True)
    pd.DataFrame(protein2go_df).to_csv(concept_path + "/relation.csv", index=False)


if __name__ == "__main__":
    uniprot()
