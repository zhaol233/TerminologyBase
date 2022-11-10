# -*- coding: utf-8 -*-
# @Time    : 2022/4/26 15:44
# @Author  : zhaoliang
# @Description: TODO

import pandas as pd
from util import generate_id_with_origin_code, add_node, add_data, add_relation, generate_id
from util import _node, _relation, log, db_code
import os

import copy

hpo_path = "./input/HPO/hp.obo"



def hpo():
    log.info("-----------HPO---------------")

    f = open(hpo_path, "r", encoding="utf-8")
    lines = f.readlines()
    f.close()

    node_seq_id = 1  # 添加偏置
    rel_seq_id = 1
    is_A = False
    status = 0

    node = copy.deepcopy(_node)
    alias_node = copy.deepcopy(_node)
    relation = copy.deepcopy(_relation)

    node.update({"is_obsolete": [], "definition": [], "replaced_by": [], "comment": []})
    node = add_data(node, ['C012100000000000', 'obsolete_node_subset', 'Concept|HPO', 'HPO', '', 'true',
                           'a self create node to link add obsolete nodes', '', ''])

    relation_id = generate_id('R', db_code["HPO"], rel_seq_id)
    rel_seq_id += 1
    relation = add_relation(relation, relation_id, 'C012100000000000', 'C012000000000001', 'is_A', 'HPO', '')
    omim_node = copy.deepcopy(_node)
    orpha_node = copy.deepcopy(_node)
    entrez_gene = copy.deepcopy(_node)

    omim2hpo = copy.deepcopy(_relation)
    orpha2hpo = copy.deepcopy(_relation)
    hp2gene = copy.deepcopy(_relation)

    node_id = 'zhaoliangID'
    is_obsolete = 'false'
    definition = ''
    replaced_by = ''
    comment = ""
    for line in lines:
        if line.startswith('[Term]') and status == 0:
            status = 1

        if line == '\n' and status == 1:
            status = 0
            node = add_data(node,
                            [node_id, node_name, 'Concept|Phenotype|HPO', 'HPO', original_id, is_obsolete, definition,
                             replaced_by, comment])
            is_obsolete = "false"
            definition = ''
            replaced_by = ""
            comment = ""
            if is_A:
                is_A = False
            else:
                if node_name == 'All':
                    pass
                else:
                    relation_id = generate_id('R', db_code['HPO'], rel_seq_id)
                    rel_seq_id += 1
                    relation = add_relation(relation, relation_id, node_id, 'C012100000000000', 'is_A', 'HPO', '')

        if line.startswith("id:"):
            original_id = line.split("d: ")[1].strip()
            node_id = generate_id_with_origin_code("C", db_code["HPO"], original_id)

        if line.startswith("name: "):
            node_name = line.split('e:')[1].strip()
            continue
        if line.startswith("def:"):
            definition = line[5:].replace('\"', '').strip()
            continue
        if line.startswith("comment:"):
            comment = line.split(": ")[1].strip()
            continue
        if line.startswith("synonym:"):
            alias = line.split(":")[1].split("\"")[1].strip()

            alias_node_id = generate_id('S', db_code["HPO"], node_seq_id)
            node_seq_id += 1
            alias_node = add_node(alias_node, alias_node_id, alias, 'Synonym|Phenotype|HPO', "HPO", "")

            relation_id = generate_id('R', db_code["HPO"], rel_seq_id)
            rel_seq_id += 1
            relation = add_relation(relation, relation_id, alias_node_id, node_id, "alias", "HPO", "")

            continue
        if line.startswith("is_a:"):
            relationed_node_original_id = line[6:].split("!")[0].strip()
            relationed_node_id = generate_id_with_origin_code('C', db_code['HPO'], relationed_node_original_id)
            relation_id = generate_id('R', db_code["HPO"], rel_seq_id)
            rel_seq_id += 1
            relation = add_relation(relation, relation_id, node_id, relationed_node_id, "is_A", "HPO", '')
            is_A = True

            continue
        if "is_obsolete: " in line:
            is_obsolete = line.split(": ")[1].strip()

            continue
        if "replaced_by: " in line:
            replaced_by = line.split(": ")[1].strip()
            continue

    log.info(f"HPO total term {len(node['node_id'])}")
    log.info(f"HPO total alias node term {len(alias_node['node_id'])}")

    concept_path = "./results/hpo/concept"
    os.makedirs(concept_path, exist_ok=True)
    pd.DataFrame(node).to_csv(concept_path + "/concept.csv", index=False)

    synonym_path = "./results/hpo/synonym"
    os.makedirs(synonym_path, exist_ok=True)
    pd.DataFrame(alias_node).to_csv(synonym_path + f"/concept.csv", index=False)

    relation_path = "./results/hpo/relation"
    os.makedirs(relation_path, exist_ok=True)
    pd.DataFrame(relation).to_csv(relation_path + f"/relation.csv", index=False)

    # 处理hpo2omim
    log.info("start to operate hpo2disease")
    mapping_id = 210033
    disease2hpo_path = "./input/HPO/phenotype.hpoa"
    hpo2gene_path = "./input/HPO/phenotype_to_genes.txt"
    phenotype = pd.read_csv(disease2hpo_path, header=0, skiprows=4, delimiter="\t",
                            usecols=["#DatabaseID", "DiseaseName", "Qualifier", "HPO_ID",
                                     "Evidence", "Onset", "Aspect"])

    OMIM = phenotype[phenotype['#DatabaseID'].str.startswith('OMIM')]
    ORPHA = phenotype[phenotype['#DatabaseID'].str.startswith('ORPHA')]

    omim_disease_name = list(OMIM['DiseaseName'])
    omim_disease_original_id = list(OMIM['#DatabaseID'])
    omim_disease_node_id = [generate_id_with_origin_code('C', db_code['OMIM'], id) for id in omim_disease_original_id]
    hpo_id = list(OMIM['HPO_ID'])
    hpo_node_id = [generate_id_with_origin_code('C', db_code['HPO'], id) for id in hpo_id]
    omim_node['node_id'] = omim_disease_node_id
    omim_node['node_name'] = omim_disease_name
    omim_node['tag'] = ['Concept|Disease|OMIM' for _ in omim_disease_original_id]
    omim_node['original_code'] = omim_disease_original_id
    omim_node['source'] = ['OMIM' for _ in omim_disease_original_id]

    omim2hpo['relation_id'] = \
        [generate_id('R', db_code['mapping'], i) for i in range(mapping_id, mapping_id + len(omim_disease_node_id))]
    omim2hpo['node_id'] = omim_disease_node_id
    omim2hpo['relationed_node_id'] = hpo_node_id
    omim2hpo['relation_tag'] = ['mapping' for _ in omim_disease_node_id]
    omim2hpo['source'] = ['HPO' for _ in omim_disease_node_id]
    omim2hpo['original_code'] = ['' for _ in omim_disease_node_id]

    relation_path = "./results/mapping/omim2hpo"
    os.makedirs(relation_path, exist_ok=True)
    pd.DataFrame(omim2hpo).to_csv(relation_path + "/relation.csv", index=False)

    concept_path = "./results/omim/concept"
    os.makedirs(concept_path, exist_ok=True)
    df_omim = pd.DataFrame(omim_node)
    df_omim.drop_duplicates(subset=['node_id'], inplace=True)
    df_omim.to_csv(concept_path + "/concept.csv", index=False)

    orpha_disease_name = list(ORPHA['DiseaseName'])
    orpha_disease_original_id = list(ORPHA['#DatabaseID'])
    orpha_disease_node_id = [generate_id_with_origin_code('C', db_code['ORPHA'], id) for id in orpha_disease_original_id]
    hpo_id = list(ORPHA['HPO_ID'])
    hpo_node_id = [generate_id_with_origin_code('C', db_code['HPO'], id) for id in hpo_id]
    orpha_node['node_id'] = orpha_disease_node_id
    orpha_node['node_name'] = orpha_disease_name
    orpha_node['tag'] = ['Concept|Disease|ORPHA' for _ in orpha_disease_original_id]
    orpha_node['original_code'] = orpha_disease_original_id
    orpha_node['source'] = ['ORPHA' for _ in orpha_disease_original_id]

    orpha2hpo['relation_id'] = \
        [generate_id('R', db_code['mapping'], i) for i in range(mapping_id, mapping_id + len(orpha_disease_node_id))]
    orpha2hpo['node_id'] = orpha_disease_node_id
    orpha2hpo['relationed_node_id'] = hpo_node_id
    orpha2hpo['relation_tag'] = ['mapping' for _ in orpha_disease_node_id]
    orpha2hpo['source'] = ['HPO' for _ in orpha_disease_node_id]
    orpha2hpo['original_code'] = ['' for _ in orpha_disease_node_id]

    relation_path = "./results/mapping/orpha2hpo"
    os.makedirs(relation_path, exist_ok=True)
    pd.DataFrame(orpha2hpo).to_csv(relation_path + "/relation.csv", index=False)

    concept_path = "./results/orpha/concept"
    os.makedirs(concept_path, exist_ok=True)
    df_orpha = pd.DataFrame(orpha_node)
    df_orpha.drop_duplicates(subset=['node_id'], inplace=True)
    df_orpha.to_csv(concept_path + "/concept.csv", index=False)

    mapping_id += len(omim_disease_node_id)

    # 处理hpo2gene
    log.info("start to operate gene")
    hpo_to_gene = pd.read_csv(hpo2gene_path, header=0, delimiter="\t",
                              usecols=["HPO-id", "HPO-label", "entrez-gene-id", "entrez-gene-symbol",
                                       'Additional-Info'])

    entrez_gene_original_id = list(hpo_to_gene['entrez-gene-id'])
    hpo_id = list(hpo_to_gene['HPO-id'])
    entrez_gene_node_id = [generate_id('C', db_code['entrez'], id) for id in entrez_gene_original_id]
    hpo_node_id = [generate_id_with_origin_code('C', db_code['HPO'], id) for id in hpo_id]
    entrez_gene_name = list(hpo_to_gene['entrez-gene-symbol'])
    additional_info = list(hpo_to_gene['Additional-Info'])

    entrez_gene['node_id'] = entrez_gene_node_id
    entrez_gene['node_name'] = entrez_gene_name
    entrez_gene['original_code'] = entrez_gene_original_id
    entrez_gene['source'] = ['Entrez' for _ in hpo_id]
    entrez_gene['tag'] = ['Concept|Gene|Entrez' for _ in hpo_id]

    hp2gene['relation_id'] = [generate_id('R', db_code['mapping'], i) for i in
                              range(mapping_id, mapping_id + len(hpo_id))]
    hp2gene['node_id'] = hpo_node_id
    hp2gene['relationed_node_id'] = entrez_gene_node_id
    hp2gene['relation_tag'] = ['mapping' for _ in hpo_id]
    hp2gene['source'] = ['HPO' for _ in hpo_id]
    hp2gene['original_code'] = ['' for _ in hpo_id]
    hp2gene.update({'additional_info': additional_info})

    mapping_id += len(node_id)
    log.info(f"finally mapping_id: {mapping_id}")

    concept_path = "./results/Entrez/concept"
    os.makedirs(concept_path, exist_ok=True)
    df_gene = pd.DataFrame(entrez_gene)
    df_gene.drop_duplicates(subset=['node_id'], inplace=True)
    df_gene.to_csv(concept_path + "/concept.csv", index=False)

    relation_path = "./results/mapping/hpo2gene"
    os.makedirs(relation_path, exist_ok=True)
    pd.DataFrame(hp2gene).to_csv(relation_path + "/relation.csv", index=False)


if __name__ == "__main__":
    hpo()
