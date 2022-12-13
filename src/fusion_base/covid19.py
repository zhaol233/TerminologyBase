# -*- coding: utf-8 -*-
# @Time    : 2022/8/18 14:40
# @Author  : zhaoliang
# @Description: TODO
import pandas as pd
from term.src.utils.util import generate_id_with_origin_code, add_node, add_data, add_relation, generate_id
from term.src.utils.util import base_node, base_relation, log, db_code, root_path
import os
import copy

def NCI():
    # C173381  --  NCIt COVID-19 Terminology
    term_df = copy.deepcopy(base_node)

    nci_concept_df = pd.read_csv('../../results/NCI/concept/concept.csv')
    nci_synonym_df = pd.read_csv('../../results/NCI/synonym/concept.csv')
    nci_relation_df = pd.read_csv('../../results/NCI/relation/relation.csv', header=0)
    all_is_a_relation = nci_relation_df.loc[nci_relation_df['relation_tag'] == "is_A", :]

    # Concept_In_Subset
    direct_relation = nci_relation_df.loc[nci_relation_df['relationed_node_id'] == "C014000000173381"]
    direct_is_a_relation = direct_relation.loc[direct_relation['relation_tag'] == "Concept_In_Subset"]

    direct_node_l = list(direct_is_a_relation['node_id'])

    second_from_node = nci_relation_df[nci_relation_df['relationed_node_id'].isin(direct_node_l)]
    second_to_node = nci_relation_df[nci_relation_df['node_id'].isin(direct_node_l)]

    relation = pd.concat([second_from_node, second_to_node, direct_is_a_relation])

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

    concept_path = root_path + "/results/merge_db/covid19/NCI/concept"
    os.makedirs(concept_path, exist_ok=True)
    concept_node.to_csv(concept_path + "/concept.csv", index=False)

    synonym_path = root_path + "/results/merge_db/covid19/NCI/synonym"
    os.makedirs(synonym_path, exist_ok=True)
    synonym_node.to_csv(synonym_path + "/concept.csv", index=False)

    relation_path = root_path + "/results/merge_db/covid19/NCI/relation"
    os.makedirs(relation_path, exist_ok=True)
    relation.to_csv(relation_path + "/relation.csv", index=False)


def uniprot_go_hgnc():
    df_covid19 = pd.read_csv(root_path + "/input/UNIPROT/covid19-2022.11.25-07.50.41.35.tsv", sep='\t', header=0)
    df_uniprot = pd.read_csv(root_path + "/results/uniprot/concept/concept.csv", header=0)
    df_relation = pd.read_csv(root_path + "/results/uniprot/relation/relation.csv", header=0)

    df_hgnc_concept = pd.read_csv(root_path + "/results/hgnc/concept/concept.csv", header=0)
    df_hgnc_synonym = pd.read_csv(root_path + "/results/hgnc/synonym/concept.csv", header=0)
    df_hgnc_relation = pd.read_csv(root_path + "/results/hgnc/relation/relation.csv", header=0)

    df_go = pd.read_csv(root_path + '/input/GO/sars-cov-2_targets.gpi', sep='\t')

    df_uniprot_go = df_uniprot.loc[df_uniprot['original_code'].isin(df_go.iloc[:, 1])]

    df_uniprot_concept = df_uniprot.loc[df_uniprot['original_code'].isin(df_covid19['Entry'])]
    df_uniprot_concept = pd.concat([df_uniprot_go,df_uniprot_concept])

    df_relation = df_relation.loc[df_relation['node_id'].isin(df_uniprot_concept['node_id'])]
    root_node = df_uniprot[df_uniprot['node_name'] == 'PROTEIN']
    df_uniprot_concept = pd.concat([df_uniprot_concept, root_node])

    concept_path = root_path + "/results/merge_db/covid19/uniprot/concept"
    os.makedirs(concept_path, exist_ok=True)
    df_uniprot_concept.to_csv(concept_path + "/concept.csv", index=False)

    relation_path = root_path + "/results/merge_db/covid19/uniprot/relation"
    os.makedirs(relation_path, exist_ok=True)
    df_relation.to_csv(relation_path + "/relation.csv", index=False)
    gene_name = []
    for i in list(df_covid19['Gene Names'].apply(lambda x: str(x).split(' '))):
        gene_name += i
    df_hgnc_covid19_concept = df_hgnc_concept.loc[df_hgnc_concept['node_name'].isin(set(gene_name))]

    df_hgnc_go = df_hgnc_concept.loc[df_hgnc_concept['node_name'].isin(df_go.iloc[:, 2])]

    df_hgnc_covid19_concept = pd.concat([df_hgnc_go, df_hgnc_covid19_concept])

    df_hgnc_covid19_relation = pd.concat([df_hgnc_relation.loc[df_hgnc_relation['node_id'].isin(df_hgnc_covid19_concept['node_id'])],
                                        df_hgnc_relation.loc[df_hgnc_relation['relationed_node_id'].isin(df_hgnc_covid19_concept['node_id'])]
                                        ])
    df_hgnc_covid19_relation.drop_duplicates(subset=['relation_id'], inplace=True)
    node_l = list(df_hgnc_covid19_relation['node_id']) + list(df_hgnc_covid19_relation['relationed_node_id'])
    df_hgnc_covid19_concept = df_hgnc_concept[df_hgnc_concept['node_id'].isin(node_l)]
    df_hgnc_covid19_synonym = df_hgnc_synonym[df_hgnc_synonym['node_id'].isin(node_l)]

    concept_path = root_path + "/results/merge_db/covid19/hgnc/concept"
    os.makedirs(concept_path, exist_ok=True)
    df_hgnc_covid19_concept.to_csv(concept_path + "/concept.csv", index=False)

    synonym_path = root_path + "/results/merge_db/covid19/hgnc/synonym"
    os.makedirs(synonym_path, exist_ok=True)
    df_hgnc_covid19_synonym.to_csv(synonym_path + "/concept.csv", index=False)

    relation_path = root_path + "/results/merge_db/covid19/hgnc/relation"
    os.makedirs(relation_path, exist_ok=True)
    df_hgnc_covid19_relation.to_csv(relation_path + "/relation.csv", index=False)


def to_root():
    relation = {"relation_id": [], "node_id": [], 'node_source': [], "relationed_node_id": [],
                "relationed_node_source": [], "relation_tag": [], "source": [], "original_code": []}

    rel_sep_id = 1

    # HGNC gene
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C009000000000000', 'HGNC', '13', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # UniProt protein
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C01100000000000000', 'UniProt', '12', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Drug, Food, Chemical or Biomedical Material --> drug
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000001908', 'NCI', '3', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Biochemical Pathway --> pathway
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000020633', 'NCI', '15', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Diagnostic or Prognostic Factor --> qualifier value
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000020047', 'NCI', '18', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Disease, Disorder or Finding --> Disease
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000007057', 'NCI', '1', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Molecular Abnormality --> variant
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000003910', 'NCI', '14', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI  Biological Process --> pathway
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000017828', 'NCI', '15', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Manufactured Object --> tool
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000097325', 'NCI', '25', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Chemotherapy Regimen or Agent Combination --> method
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000012218', 'NCI', '24', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Abnormal Cell --> cell
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000012913', 'NCI', '11', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI  Gene Product --> gene
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000026548', 'NCI', '13', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Property or Attribute --> qualifier value
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000020189', 'NCI', '18', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Activity --> other operation
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000043431', 'NCI', '6', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Organism --> body structure
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000014250', 'NCI', '7', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Gene --> gene
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000016612', 'NCI', '13', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Anatomic Structure, System, or Substance --> body structure
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000012219', 'NCI', '7', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    # NCI Conceptual Entity --> qualifier value
    relation_id = generate_id('R', db_code['covid19'], rel_sep_id)
    rel_sep_id += 1
    relation = add_data(relation, [relation_id, 'C014000000020181', 'NCI', '18', "COVID-19 Base", 'is_A', "COVID-19 Base", ''])

    relation_path = root_path + "/results/merge_db/covid19"
    os.makedirs(relation_path, exist_ok=True)
    pd.DataFrame(relation).to_csv(relation_path + "/relation.csv", index=False)


if __name__ == '__main__':
    # NCI()
    # uniprot_go_hgnc()
    to_root()
    # pass
