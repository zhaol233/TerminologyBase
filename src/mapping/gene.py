# -*- coding: utf-8 -*-
# @Time    : 2022/5/9 17:24
# @Author  : zhaoliang
# @Description: TODO
import os

import pandas as pd
import numpy as np
import datetime
from term.src.utils.util import base_node, base_relation, log, db_code


df_hgnc_gene = root_path + "/results/HGNC/concept.csv"
df_uniprot_protein = root_path + '/results/uniprot/uniprot_node_93114.csv'

entrez_gene = '../results/hpo/entrez_gene.csv'

# mapping
gene2go = root_path + '/results/mapping/gene2go/relation_raw.csv'
protein2go = root_path + '/results/mapping/protein2go/relation_raw.csv'
gene2protein = root_path + '/results/mapping/gene2go/relation_raw.csv'

id_start = 1000000


def mapping_gene2go():
    pass


def gene_merge():
    df_hgnc = pd.read_csv(hgnc_gene, sep=',', header=0)
    df_nci = pd.read_csv(nci_gene, sep=',', header=0)
    gene_map = {"node_id": [], "relation_node_id": []}
    hgnc_entrez_map = {"node_id": [], "relation_node_id": []}
    df_nci.dropna(subset=['HGNC_ID'], axis=0, inplace=True)
    df_hgnc.set_index('original_code', inplace=True)
    for idx, row in df_nci.iterrows():
        nci_hgnc_id = row.loc['HGNC_ID']
        if nci_hgnc_id in df_hgnc.index:
            node_id = row.loc['node_id']
            relation_node_id = df_hgnc.loc[nci_hgnc_id, 'node_id']
            gene_map['node_id'].append(node_id)
            gene_map['relation_node_id'].append(relation_node_id)
    df_gene_map = pd.DataFrame(gene_map)
    global id_start
    print("df_gene_map", id_start, id_start + df_gene_map.shape[0])
    relation_id = [generate_id('R',db_code['mapping'],id) for id in range(id_start,id_start+df_gene_map.shape[0])]
    id_start += df_gene_map.shape[0]
    df_gene_map.insert(1, 'relation_id', relation_id)
    df_gene_map.insert(3, 'mapping_rule', 'same_as')
    df_gene_map.insert(4, 'create_date', datetime.datetime.now().strftime('%Y-%m-%d'))
    df_gene_map.to_csv(res_dir + f'/nci_hgnc_gene_mapping_{df_gene_map.shape[0]}.csv', index=False)
    #
    df_hgnc.dropna(subset=['entrez_gene_id'], inplace=True)
    global entrez_gene
    df_entrez = pd.read_csv(entrez_gene, sep=',', header=0)
    df_entrez.set_index('original_node', inplace=True)
    # entrez_gene = list(df_entrez['original_code'])
    #
    for idx, row in df_hgnc.iterrows():
        hgnc_entrez_id = row.loc['entrez_gene_id']
        if hgnc_entrez_id in df_entrez.index:
            node_id = row.loc['node_id']
            relation_node_id = df_entrez.loc[hgnc_entrez_id, 'node_id']
            hgnc_entrez_map['node_id'].append(node_id)
            hgnc_entrez_map['relation_node_id'].append(relation_node_id)

    df_hgnc_entrez = pd.DataFrame(hgnc_entrez_map)

    # global id_start
    print("df_hgnc_entrez", id_start, id_start+df_hgnc_entrez.shape[0])
    relation_id = [generate_id('R',db_code['mapping'], id) for id in range(id_start,id_start+df_hgnc_entrez.shape[0])]

    id_start += df_hgnc_entrez.shape[0]

    df_hgnc_entrez.insert(1, 'relation_id', relation_id)
    df_hgnc_entrez.insert(3, 'mapping_rule', 'same_as')
    df_hgnc_entrez.insert(4, 'create_date', datetime.datetime.now().strftime('%Y-%m-%d'))
    df_hgnc_entrez.to_csv(res_dir + f'/hgnc_entrez_gene_mapping_{df_hgnc_entrez.shape[0]}.csv', index=False)


def protein_merge():
    df_uniprot = pd.read_csv(uniprot_protein, sep=',', header=0)
    df_coding_protein = pd.read_csv(coding_protein, sep=',', header=0,usecols=['node_id','relation_node_name'])
    df_nci = pd.read_csv(nci_gene_product, sep=',', header=0)
    protein = {"node_id": [], "relation_node_id": []}
    df_nci.dropna(subset=['Swiss_Prot'], axis=0, inplace=True)
    df_uniprot.set_index('original_code', inplace=True)
    for idx, row in df_nci.iterrows():
        protein_id = row.loc['Swiss_Prot']
        if protein_id in df_uniprot.index:
            node_id = row.loc['node_id']
            relation_node_id = df_uniprot.loc[protein_id, 'node_id']

            protein['node_id'].append(node_id)
            protein['relation_node_id'].append(relation_node_id)

    df_nci_uniprot = pd.DataFrame(protein)

    global id_start
    print("nci_uniprot_protein_mapping", id_start, id_start+df_nci_uniprot.shape[0])
    relation_id = [generate_id('R',db_code['mapping'],id) for id in range(id_start,id_start+df_nci_uniprot.shape[0])]
    id_start += df_nci_uniprot.shape[0]

    df_nci_uniprot.insert(1, 'relation_id', relation_id)
    df_nci_uniprot.insert(3, 'mapping_rule', 'gene_coding_protein')
    df_nci_uniprot.insert(4, 'create_date', datetime.datetime.now().strftime('%Y-%m-%d'))
    df_nci_uniprot.to_csv(res_dir + f'/nci_uniprot_protein_mapping_{df_nci_uniprot.shape[0]}.csv', index=False)

    protein_id = []
    for idx, row in df_coding_protein.iterrows():
        protein_name = row['relation_node_name']
        if protein_name in df_uniprot.index:
            protein_id.append(df_uniprot.loc[protein_name, 'node_id'])
    # df_coding_protein.drop(columns=['relation_node_id', 'node_name', 'symbol','relation_node_name',
    # 'relation_id','source',], inplace=True)
    print("df_coding_protein", id_start,id_start+df_coding_protein.shape[0])
    relation_id = [generate_id('R', db_code['mapping'], id) for id in range(id_start,id_start+df_coding_protein.shape[0])]
    id_start += df_coding_protein.shape[0]

    df_coding_protein.insert(1, 'relation_id', relation_id)
    df_coding_protein.insert(3, 'mapping_rule', 'gene_coding_protein')
    df_coding_protein.insert(4, 'create_date', datetime.datetime.now().strftime('%Y-%m-%d'))
    df_coding_protein.to_csv(res_dir + '/hgnc_gene_coding_protein_merged.csv', index=False)


if __name__ == "__main__":
    gene_merge()
    print('####id####', id_start)  # 184429
    protein_merge()
    print('####id####', id_start)  # 210033

    # hpo 相关  后 id 1490673

