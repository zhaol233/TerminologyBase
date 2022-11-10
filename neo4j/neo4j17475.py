# -*- coding: utf-8 -*-
# @Time    : 2022/8/24 19:23
# @Author  : zhaoliang
# @Description: import NCI
import os

import pandas as pd
from py2neo import Graph

from neo4j import create_relation_from_csv, create_node_from_csv_for17475, create_index, to_root, add_label,\
    add_related_node_label

local_dir = "../results"
g = Graph("http://172.20.137.104:17475", password="covid-19")
print(local_dir)
TYPE = {
    'Concept': 'cid',
    'Description': 'did',
    'Synonym': 'sid',
    'Relation': 'rid'
}


def import_nci():
    concept_path = '/NCI/top_concept.csv'
    top_relation = '/NCI/top_relation.csv'

    # create_node_from_csv_for17475(concept_path, 'Concept:NCI:top_concept', 'Concept')
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
    for type in subtype:
        concept_all_path = f'{local_dir}/NCI/{type}'
        file = os.listdir(concept_all_path)
        for concept in file:
            concept_path = '/NCI/' + type + f'/{concept}'
            if type == 'Activity1':
                if concept.startswith('term'):
                    create_node_from_csv_for17475(concept_path, f'Concept:NCI:Activity', 'Concept')
                elif concept.startswith('alias_node'):
                    create_node_from_csv_for17475(concept_path, f'Synonym:NCI:Activity', 'Synonym')
                continue
            if type == 'Drug_Food_Chemical_or_Biomedical_Material1':
                if concept.startswith('term'):
                    create_node_from_csv_for17475(concept_path,
                                                  f'Concept:NCI:Drug_Food_Chemical_or_Biomedical_Material', 'Concept')
                elif concept.startswith('alias_node'):
                    create_node_from_csv_for17475(concept_path,
                                                  f'Synonym:NCI:Drug_Food_Chemical_or_Biomedical_Material', 'Synonym')
                continue
            if type == 'Disease_Disorder_or_Finding1':
                if concept.startswith('term'):
                    create_node_from_csv_for17475(concept_path, f'Concept:NCI:Disease_Disorder_or_Finding', 'Concept')
                elif concept.startswith('alias_node'):
                    create_node_from_csv_for17475(concept_path, f'Synonym:NCI:Disease_Disorder_or_Finding', 'Synonym')
                continue

            if concept.startswith('term'):
                create_node_from_csv_for17475(concept_path, f'Concept:NCI:{type}', 'Concept')
            if concept.startswith('alias_node'):
                create_node_from_csv_for17475(concept_path, f'Synonym:NCI:{type}', 'Synonym')

    # 导入关系
    for type in subtype:
        concept_all_path = f'{local_dir}/NCI/{type}'
        file = os.listdir(concept_all_path)
        for relation_file in file:
            if relation_file.startswith('term') or relation_file.startswith('test') or relation_file.startswith(
                    'relation') or relation_file.startswith('alias_node'):
                continue
            relation = relation_file.split('.')[0]
            relation = relation.replace('-', '_')
            relation_path = f'/NCI/{type}/{relation_file}'
            print(relation_path)
            create_relation_from_csv(relation_path, relation, 'NCI', 'NCI')


def covid():
    root_name = {
        'Activity': 'other operation',
        'Amino-Acid-Peptide-or-Protein-Enzyme-Pharmacologic-Substance': 'protein',
        'Amino-Acid-Peptide-or-Protein-Enzyme': 'protein',
        'Amino-Acid-Peptide-or-Protein-Hormone': 'protein',
        'Amino-Acid-Peptide-or-Protein-Immunologic-Factor-Pharmacologic-Substance': 'protein',
        'Amino-Acid-Peptide-or-Protein-Immunologic-Factor-Receptor': 'protein',
        'Amino-Acid-Peptide-or-Protein-Immunologic-Factor': 'protein',
        'Amino-Acid-Peptide-or-Protein-Pharmacologic-Substance': 'protein',
        'Amino-Acid-Peptide-or-Protein-Receptor': 'protein',
        'Amino-Acid-Peptide-or-Protein': 'protein',
        'Antibiotic-Organic-Chemical-Pharmacologic-Substance': 'drug',
        'Antibiotic-Pharmacologic-Substance': 'drug',
        'Bacterium-Immunologic-Factor-Pharmacologic-Substance': 'drug',
        'Bacterium-Immunologic-Factor': 'microbe', 'Bacterium-Pharmacologic-Substance': 'drug',
        'Biologically-Active-Substance': 'microbe', 'Body-Substance': 'substance',
        'Cell-Pharmacologic-Substance': 'cell', 'Classification': 'method',
        'Clinical-Attribute': 'clinical attribute',
        'Conceptual-Entity': 'qualifier value', 'Disease-or-Syndrome': 'disease', 'Finding': 'finding',
        'Gene-or-Genome': 'gene',
        'Geographic-Area': 'environment or geographical location', 'Health-Care-Activity': 'method',
        'Health-Care-Related-Organization': 'organization',
        'Immunologic-Factor-Pharmacologic-Substance': 'drug',
        'Individual-Behavior': 'behavior or event', 'Inorganic-Chemical-Pharmacologic-Substance': 'drug',
        'Intellectual-Product': 'finding', 'Laboratory-or-Test-Result': 'finding',
        'Laboratory-Procedure': 'exam',
        'Manufactured-Object': 'equipment', 'Molecular-Biology-Research-Technique': 'pathway',
        'Nucleic-Acid-Nucleoside-or-Nucleotide-Pharmacologic-Substance': 'drug',
        'Organ-or-Tissue-Function': 'symptom or sign',
        'Organic-Chemical-Pharmacologic-Substance-Vitamin': 'drug',
        'Organic-Chemical-Pharmacologic-Substance': 'drug', 'Organic-Chemical-Vitamin': '',
        'Organic-Chemical': '', 'Pathologic-Function': 'behavior or event',
        'Pharmacologic-Substance-Virus': 'drug',
        'Pharmacologic-Substance-Vitamin': 'drug',
        'Pharmacologic-Substance': 'drug',
        'Qualitative-Concept': 'qualifier value', 'Quantitative-Concept': 'qualifier value',
        'Research-Activity': 'other operation', 'Sign-or-Symptom': 'symptom or sign',
        'Spatial-Concept': 'environment or geographical location',
        'Therapeutic-or-Preventive-Procedure': 'method',
        'Virus': 'microbe',
        'Vitamin': 'drug'}

    # alias node
    # create_node_from_csv_for17475("/covid/covid_alias_node.csv", 'NCI:Synonym','Synonym')

    # node to_root
    covid_file_path = local_dir + "/NCI/covid/"
    for file in os.listdir(covid_file_path):

        subtype_name = file.split('.')[0]
        if subtype_name in ['covid_alias','covid_is_a_node','covid_is_a','covid_alias_node','covid_subnode']:
            continue
        if root_name[subtype_name] != '':
            pass
            file_path = f"/covid/{file}"
            # print(file_path)
            # create_node_from_csv_for17475(file_path, f"NCI:{root_name[subtype_name].replace(' ','_').capitalize()}:Concept",'Concept')
            # to_root(file_path,"NCI:Concept","Concept",root_name[subtype_name])

    # TODO:需要删除到Concept节点的is_A关系
    # create_relation_from_csv("/covid/covid_is_a.csv","is_A", "NCI:Concept","NCI:Concept")

    # alias rel
    create_relation_from_csv("/covid/covid_alias.csv",'alias',"NCI:Concept","NCI:Synonym")

def new_NCI():
    pass


if __name__ == '__main__':
    # import_nci()   # pass
    # covid()
    pass

