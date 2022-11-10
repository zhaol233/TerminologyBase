# -*- coding: utf-8 -*-
# @Time    : 2022/8/24 20:36
# @Author  : zhaoliang
# @Description: TODO


import copy
import os

import pandas as pd

from py2neo import Graph

local_dir = "../results"
g = Graph("http://172.20.137.104:17474", password="clinical-term")
print(local_dir)
print("-------")
TYPE = {
    'Concept': 'cid',
    'Description': 'did',
    'Synonym': 'sid',
    'Relation': 'rid'
}

def import_hgnc():
    concept_path = '/hgnc/concept.csv'
    synonym_path = '/hgnc/synonym.csv'

    is_A = '/hgnc/is_A.csv'
    alias = '/hgnc/alias.csv'

    # create_node_from_csv(concept_path, 'Concept:HGNC:GENE', 'Concept')
    # create_node_from_csv(synonym_path, 'Synonym:HGNC:GENE', 'Synonym')
    # # create_index('HGNC')
    # #
    #
    create_relation_from_csv(is_A, 'is_A', 'HGNC', 'HGNC')
    create_relation_from_csv(alias, 'alias', 'HGNC', 'HGNC')


def import_hpo():
    concept_path = "/hpo/hpo_node.csv"
    synonym_path = '/hpo/hpo_alias_node.csv'
    omim_path = "/hpo/omim_12469.csv"
    entrez_path = '/hpo/entrez_gene.csv'

    hpo2gene = '/hpo/hpo2gene.csv'
    hpo2omim = '/hpo/hpo2omim.csv'

    # create_node_from_csv(concept_path, 'Concept:HPO:Phenotype', 'Concept')
    # create_node_from_csv(synonym_path, 'Synonym:HPO:Phenotype', 'Synonym')
    # create_node_from_csv(omim_path, 'Concept:OMIM:Disease', 'Concept')
    # # create_node_from_csv(entrez_path, 'Concept:Entrez:GENE', 'Concept')
    #
    # create_index('HPO')
    # create_index("OMIM")
    # create_index('Entrez')

    # create_relation_from_csv(hpo2omim, 'hpo_indicate_disease', 'HPO', 'OMIM')
    # create_relation_from_csv(hpo2gene, 'hpo_indicate_gene', 'HPO', 'Entrez')

    for relation in get_file_from_dir('/hpo/hpo_relation_split'):
        relation_name = os.path.basename(relation).split('.')[0]
        create_relation_from_csv(relation, relation_name, 'HPO', 'HPO')


def import_uniprot():
    concept_path = '/uniprot/uniprot_node_93114.csv'
    is_A = '/uniprot/is_A_93113.csv'

    # create_node_from_csv(concept_path, 'Concept:UniProt:Protein', 'Concept')
    create_index('UniProt')
    create_relation_from_csv(is_A, 'is_A', 'UniProt', 'UniProt')


def import_go():
    concept_path = '/go/go_node_47262.csv'
    synonym = '/go/go_alias_node_127817.csv'

    # create_node_from_csv(concept_path, 'Concept:GO:Ontology', 'Concept')
    # create_node_from_csv(synonym, 'Synonym:GO:Ontology', 'Synonym')
    # create_index('GO')
    #
    # for relation in get_file_from_dir('/go/Go_relation_split'):
    #     relation_name = os.path.basename(relation).split('.')[0]
    #     create_relation_from_csv(relation, relation_name, 'GO', 'GO')
    create_relation_from_csv('/go/Go_relation_split/GO_positively_regulates.csv', 'GO_positively_regulates', 'GO', 'GO')
    create_relation_from_csv('/go/Go_relation_split/GO_regulates.csv', 'GO_regulates', 'GO', 'GO')
    create_relation_from_csv('/go/Go_relation_split/is_A.csv', 'is_A', 'GO', 'GO')


def import_nci():
    concept_path = '/NCI/top_concept.csv'
    top_relation = '/NCI/top_relation.csv'

    # create_node_from_csv(concept_path, 'Concept:NCI:top_concept', 'Concept')
    # create_relation_from_csv(top_relation, 'is_A', 'NCI', 'NCI')
    # create_index('NCI')
    subtype = [
        'Abnormal_Cell',
        'Activity',
        'Activity1',
        'Anatomic_Structure_System_or_Substance',
        'Biochemical_Pathway',
        'Biological_Process',
        'Chemotherapy_Regimen',
        'Chemotherapy_Regimen_or_Agent_Combination',
        'Conceptual_Entity',
        'Diagnostic_or_Prognostic_Factor',
        'Drug_Food_Chemical_or_Biomedical_Material',
        'Drug_Food_Chemical_or_Biomedical_Material1',
        'Experimental_Organism_Anatomical_Concept',
        'Experimental_Organism_Diagnosis',
        'Gene',
        'Gene_Product',
        'Manufactured_Object',
        'Molecular_Abnormality',
        'NCI_Administrative_Concept',
        'Neoplasm',
        'Organism',
        'Pharmacologic_Substance',
        'Property_or_Attribute',

        'Disease_Disorder_or_Finding',
        'Disease_Disorder_or_Finding1',
    ]
    # 导入node
    # for type in subtype:
    #     concept_all_path = f'./results/NCI/{type}'
    #     file = os.listdir(concept_all_path)
    #     for concept in file:
    #         concept_path = '/NCI/' + type + f'/{concept}'
    #         if type == 'Activity1':
    #             # if concept.startswith('term'):
    #             #     create_node_from_csv(concept_path, f'Concept:NCI:Activity', 'Concept')
    #             if concept.startswith('alias_node'):
    #                 create_node_from_csv(concept_path, f'Synonym:NCI:Activity', 'Synonym')
    #             continue
    #         if type == 'Drug_Food_Chemical_or_Biomedical_Material1':
    #             # if concept.startswith('term'):
    #             #     create_node_from_csv(concept_path, f'Concept:NCI:Drug_Food_Chemical_or_Biomedical_Material', 'Concept')
    #             if concept.startswith('alias_node'):
    #                 create_node_from_csv(concept_path, f'Synonym:NCI:Drug_Food_Chemical_or_Biomedical_Material', 'Synonym')
    #             continue
    #         if type == 'Disease_Disorder_or_Finding1':
    #             # if concept.startswith('term'):
    #             #     create_node_from_csv(concept_path, f'Concept:NCI:Disease_Disorder_or_Finding', 'Concept')
    #             if concept.startswith('alias_node'):
    #                 create_node_from_csv(concept_path, f'Synonym:NCI:Disease_Disorder_or_Finding', 'Synonym')
    #             continue
    #
    #         # if concept.startswith('term'):
    #         #     create_node_from_csv(concept_path, f'Concept:NCI:{type}', 'Concept')
    #         if concept.startswith('alias_node'):
    #             create_node_from_csv(concept_path, f'Synonym:NCI:{type}', 'Synonym')

    # 导入关系
    for type in subtype:
        concept_all_path = f'./results/NCI/{type}'
        file = os.listdir(concept_all_path)
        for concept in file:
            if concept.startswith('term') or concept.startswith('test') or concept.startswith('relation') or concept.startswith('alias_node'):
                continue

            relation = concept.split('.')[0]
            relation = relation.replace('-', '_')
            if relation == 'alias':
                relation_path = f'/NCI/{type}/{concept}'
                print(relation_path)
                create_relation_from_csv(relation_path, relation, 'NCI', 'NCI')


def create_mapping_relation(file_path, label1, label2):
    cypher = """LOAD CSV WITH HEADERS FROM "file:%s" AS row
    MATCH (s:%s{import_id: row.node_id})
    MATCH (e:%s{import_id: row.relation_node_id})
    MERGE (s)-[r:Mapping]-(e)
    ON CREATE SET r = {
           rid: row.relation_id, history_id: [], relation_id: row.relation_id, node_id: row.node_id,  
           import_id: row.relation_id,relation_node_id: row.relation_node_id, relation_tag: \'Mapping\',  
           source: \'Biology and Medical Terminology Base\', 
        mapping_rule: row.mapping_rule, active: 2, original_code: \'\',  
        create_date: row.create_date }
    RETURN count(r);
    """ % (file_path, label1, label2)
    print(cypher)
    g.run(cypher)


def import_mapping():
    # hgnc_entrez_gene_mapping_4703 = "/mapping/hgnc_entrez_gene_mapping_4703.csv"
    # create_mapping_relation(hgnc_entrez_gene_mapping_4703, 'HGNC', 'Entrez')

    # nci_hgnc_gene_mapping_5451 = "/mapping/nci_hgnc_gene_mapping_5451.csv"
    # create_mapping_relation(nci_hgnc_gene_mapping_5451, 'NCI', 'HGNC')

    # nci_uniprot_protein_mapping_5295 = "/mapping/nci_uniprot_protein_mapping_5295.csv"
    # create_mapping_relation(nci_uniprot_protein_mapping_5295, 'NCI', 'UniProt')

    # hgnc_gene_coding_protein_merged = '/mapping/hgnc_gene_coding_protein_merged.csv'
    # create_mapping_relation(hgnc_gene_coding_protein_merged, 'HGNC', 'UniProt')

    # hpo2gene_1044546 = '/mapping/hpo2gene_1044546.csv'
    # create_mapping_relation(hpo2gene_1044546, 'HPO', 'Entrez')
    # exit()

    # hpo2omim_236094 = '/mapping/hpo2omim_236094.csv'
    # create_mapping_relation(hpo2omim_236094, 'HPO', 'OMIM')
    pass


def create_to_main_tree(label1,node1,label2, node2):
    cypher = """
    MATCH (s:%s{name:'%s'})
    MATCH (e:%s{name: '%s'})
    MERGE (s)-[r:is_A]-(e)
    on create set r = {history_id: [], import_id: '', source: 'Biology and Medical Terminology_Base'}
    return count(r);
    """%(label1,node1,label2, node2)
    print(cypher)
    g.run(cypher)


def import_to_merge_database():
    # create_to_main_tree('HGNC','GENE','Biology_and_Medical_Terminology_Base','Genetic material')

    # create_to_main_tree('UniProt','PROTEIN','Biology_and_Medical_Terminology_Base','Biological substance')
    #
    # create_to_main_tree('Root','GO','Biology_and_Medical_Terminology_Base','Functional genome')
    # create_to_main_tree('NCI','Biochemical_Pathway','Biology_and_Medical_Terminology_Base','Functional genome')

    # create_to_main_tree('NCI','Biological_Process','Biology_and_Medical_Terminology_Base','Activity')
    #
    # create_to_main_tree('HPO','human phenotype','Biology_and_Medical_Terminology_Base','Abnormal')
    create_to_main_tree('NCI','Abnormal_Cell','Biology_and_Medical_Terminology_Base','Abnormal')
    create_to_main_tree('NCI','Molecular_Abnormality','Biology_and_Medical_Terminology_Base','Abnormal')



if __name__ == '__main__':

    # import_hgnc()  # pass
    # import_hpo()
    # delete_hpo()
    print('liang')
    # reverse_rel()
    # import_uniprot() # pass
    # import_go()  # pass
    # import_nci() # pass
    # import_mapping()

    # import_to_merge_database()