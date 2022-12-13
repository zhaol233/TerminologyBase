# -*- coding: utf-8 -*-
# @Time    : 2022/4/25 18:01
# @Author  : zhaoliang
# @Description: TODO
import copy

import pandas as pd
from term.src.utils.util import generate_id_with_origin_code, add_node, add_data, add_relation, generate_id
from term.src.utils.util import base_node, base_relation, log, db_code, root_path
import os

# save_path = "./results/go"
# os.makedirs(save_path, exist_ok=True)

go_path = root_path + "/input/GO/go.obo"
go_protein_path = root_path + "/input/GO/goa_human.gaf"  # go protein


def go():
    log.info("-----------GO---------------")
    f = open(go_path, "r", encoding="utf-8")
    lines = f.readlines()
    f.close()

    node = copy.deepcopy(base_node)
    alias_node = copy.deepcopy(base_node)
    relation = copy.deepcopy(base_relation)
    protein2go_relation = copy.deepcopy(base_relation)
    gene2go_relation = copy.deepcopy(base_relation)

    # DB DB code DBObjectSymbol Qualifier GO_ID Evidence_Code
    go_protein_df = pd.read_csv(go_protein_path, sep='\t', skiprows=41, usecols=[0, 1, 2, 3, 4, 6, 8], header=None)

    node.update({"class": [], "is_obsolete": [], "definition": []})
    is_A = False

    node_seq_id = 1  # 添加偏置
    rel_seq_id = 1

    all_node_id = [generate_id_with_origin_code("C", db_code["GO"], id) for id in list(go_protein_df[4])]
    protein_name = list(go_protein_df[1])
    relation_tag = list(go_protein_df[3])
    protein_tag = ['Protein_' + i.replace('NOT|', 'not_') for i in relation_tag]

    relation_id = [generate_id("R", db_code["GO"], i) for i in range(1, go_protein_df.shape[0] + 1)]
    rel_seq_id += go_protein_df.shape[0] + 1

    protein2go_relation['relation_id'] = relation_id
    protein2go_relation['node_id'] = protein_name
    protein2go_relation['relationed_node_id'] = all_node_id
    protein2go_relation['relation_tag'] = protein_tag
    protein2go_relation['source'] = ['GO' for _ in relation_tag]
    protein2go_relation['original_code'] = ['' for _ in relation_tag]

    protein2go_relation_path = root_path + "/results/mapping/protein2go"
    os.makedirs(protein2go_relation_path, exist_ok=True)
    pd.DataFrame(protein2go_relation).to_csv(protein2go_relation_path + f"/relation_raw.csv", index=False)

    namespace_dict = {"biological_process": "C010000000008150", "molecular_function": 'C010000000003674',
                      "cellular_component": "C010000000005575"}

    is_obsolete = "false"

    node_id = 'zhaoliangID'

    status = 0

    for line in lines:
        if line.startswith('[Term]') and status == 0:
            status = 1

        if line == '\n' and status == 1:
            status = 0
            node = add_data(node, [node_id, node_name, 'Concept|Ontology|GO', 'GO', original_id, namespace, is_obsolete,
                                   definition])
            is_obsolete = "false"
            if is_A:
                is_A = False
                pass
            else:
                if node_id not in list(namespace_dict.values()):
                    relation_id = generate_id('R', db_code['GO'], rel_seq_id)
                    rel_seq_id += 1
                    relation = add_relation(relation, relation_id, node_id, namespace_dict[namespace], 'is_A', 'GO', '')

        if line.startswith("id") and status == 1:
            original_id = line.split(": ")[1].strip()
            node_id = generate_id_with_origin_code("C", db_code["GO"], original_id)
            continue
        if "[Typedef]" in line:
            break
        if "name:" in line:
            node_name = line.split('ame:')[1].strip()
            continue
        if line.startswith("namespace:"):
            namespace = line.split(":")[1].strip()
            continue
        if line.startswith("def:"):
            definition = line.split(": ")[1].split("\"")[1]
            continue
        if line.startswith("synonym:"):
            alias_name = line.split("m:")[1].split("\"")[1].strip()
            alias_node_id = generate_id('S', db_code["GO"], node_seq_id)
            node_seq_id += 1
            alias_node = add_node(alias_node, alias_node_id, alias_name, 'Synonym|Ontology|GO', "GO", '')
            relation_id = generate_id('R', db_code["GO"], rel_seq_id)
            rel_seq_id += 1
            relation = add_relation(relation, relation_id, alias_node_id, node_id, "alias", "GO", "")
            continue
        if line.startswith("is_a:"):
            relation_node_id = line.split("a:")[1].split('!')[0].strip()
            relationed_node_id = generate_id_with_origin_code('C', db_code['GO'], relation_node_id)
            is_A = True
            relation_id = generate_id('R', db_code["GO"], rel_seq_id)
            rel_seq_id += 1
            relation = add_relation(relation, relation_id, node_id, relationed_node_id, "is_A", "GO", "")
            continue

        # 内部层级关系
        if line.startswith("relationship:"):
            relationship = line.split(":")[1].strip().split()[0]
            relation_node_id = line.split("p: ")[1].split(' ')[1].strip()

            relationed_node_id = generate_id_with_origin_code('C', db_code['GO'], relation_node_id)
            relationship = "GO_" + relationship
            relation_id = generate_id('R', db_code["GO"], rel_seq_id)
            rel_seq_id += 1
            relation = add_relation(relation, relation_id, node_id, relationed_node_id, relationship, "GO", "")
            continue
        if "is_obsolete: " in line:
            is_obsolete = line.split(": ")[1].strip()
            continue

    concept_path = root_path + "/results/go/concept"
    os.makedirs(concept_path, exist_ok=True)
    pd.DataFrame(node).to_csv(concept_path + "/concept.csv", index=False)

    synonym_path = root_path + "/results/go/synonym"
    os.makedirs(synonym_path, exist_ok=True)
    pd.DataFrame(alias_node).to_csv(synonym_path + f"/concept.csv", index=False)

    relation_path = root_path + "/results/go/relation"
    os.makedirs(relation_path, exist_ok=True)
    pd.DataFrame(relation).to_csv(relation_path + f"/relation.csv", index=False)

    gene_name = list(go_protein_df[2])
    gene_tag = ['Gene_' + i.replace('NOT|', 'not_') for i in relation_tag]

    relation_id = [generate_id("R", db_code["GO"], i) for i in range(rel_seq_id, rel_seq_id + go_protein_df.shape[0])]
    rel_seq_id += go_protein_df.shape[0] + 1

    print(len(relation_id), len(relation_id), len(gene_tag), len(all_node_id))
    gene2go_relation['relation_id'] = relation_id
    gene2go_relation['node_id'] = gene_name
    gene2go_relation['relationed_node_id'] = all_node_id
    gene2go_relation['relation_tag'] = gene_tag
    gene2go_relation['source'] = ['GO' for _ in gene_tag]
    gene2go_relation['original_code'] = ['' for _ in gene_tag]

    gene2go_relation_path = root_path + "/results/mapping/gene2go"
    os.makedirs(gene2go_relation_path, exist_ok=True)
    df_gene2go_relation = pd.DataFrame(gene2go_relation)
    df_gene2go_relation.dropna(subset=['node_id'], axis=0, inplace=True, how='any')

    df_gene2go_relation.to_csv(gene2go_relation_path + f"/relation_raw.csv", index=False)


if __name__ == "__main__":
    go()
