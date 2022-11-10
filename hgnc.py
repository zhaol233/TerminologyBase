# -*- coding: utf-8 -*-
# @Time    : 2022/4/25 17:04
# @Author  : zhaoliang
# @Description: TODO
import copy

import pandas as pd
from util import *
from util import _node, _relation
import os
import json

hgnc_path = "./input/HGNC/hgnc_complete_set_2022-01-01.json"


def hgnc():
    log.info("-----------HGNC---------------")
    f = open(hgnc_path, 'r', encoding='UTF-8')
    data = json.load(f)["response"]["docs"]
    f.close()
    node = copy.deepcopy(_node)
    alias_node = copy.deepcopy(_node)
    relation = copy.deepcopy(_relation)
    coding_protein = copy.deepcopy(_relation)

    node.update({"full_name": [], "gene_group": [], "location": [], "class": [], "prename": [], "presymbol": [],
                 "OMIM_id": [], 'entrez_gene_id': []})

    coding_protein.update({"node_name": [], "relation_node_name": []})

    node_seq_id = 1
    rel_seq_id = 1

    for i in range(len(data)):
        gene_group = data[i]["gene_group"][0] if "gene_group" in data[i].keys() else ""
        locus_group = data[i]["locus_group"] if "locus_group" in data[i].keys() else ""
        full_name = data[i]["name"]
        hgnc_id = data[i]["hgnc_id"]
        node_id = generate_id_with_origin_code('C', db_code['HGNC'], hgnc_id)
        location = data[i]["location"] if "location" in data[i].keys() else ""
        prename = data[i]["prev_name"] if "prev_name" in data[i].keys() else []
        presymbol = data[i]["prev_symbol"] if "prev_symbol" in data[i].keys() else []
        omim_id = data[i]["omim_id"][0] if "omim_id" in data[i].keys() else ""
        node_name = data[i]["symbol"]
        entrez_gene_id = data[i]['entrez_id'] if 'entrez_id' in data[i].keys() else ""

        prename_s = ""
        if len(prename) != 0:
            for i_ in prename:
                prename_s += str(i_)
                prename_s += "|"
            prename_s = prename_s[:-1]
        presymbol_s = ""
        if len(presymbol) != 0:
            for i_ in presymbol:
                presymbol_s += i_
                presymbol_s += '|'
            presymbol_s = presymbol_s[:-1]

        if "alias_symbol" in data[i].keys():
            for alias in data[i]["alias_symbol"]:
                alias_node_id = generate_id("S", db_code["HGNC"], node_seq_id)
                node_seq_id += 1
                alias_node = add_node(alias_node, alias_node_id, alias, "Synonym|Gene|HGNC", "HGNC", "")

                relation_id = generate_id('R', db_code["HGNC"], rel_seq_id)
                rel_seq_id += 1
                relation = add_relation(relation, relation_id, alias_node_id, node_id, "alias", "HGNC", "")

        if "uniprot_ids" in data[i].keys():
            for protein_original_code in data[i]['uniprot_ids']:
                relation_id = generate_id('R', db_code["HGNC"], rel_seq_id)
                rel_seq_id += 1

                coding_protein = add_data(coding_protein, [relation_id, node_id, "PROTEIN_ID", "Gene_coding_protein",
                                                           "HGNC", "", node_name, protein_original_code])

        node = add_data(node, [node_id, node_name, "Concept|Gene|HGNC", 'HGNC', hgnc_id, full_name,gene_group,location,
                        locus_group,prename_s,presymbol_s, omim_id,entrez_gene_id])

        relation_id = generate_id('R', db_code["HGNC"], rel_seq_id)
        rel_seq_id += 1

        relation = add_relation(relation, relation_id, node_id, "C009000000000000", "is_A", "HGNC")

    node = add_data(node, ["C009000000000000", "GENE", "Concept|Gene|HGNC", 'HGNC', "", "Virtual GENE Root Node", "", "",
                    "", "", "", "", ""])

    log.info(f"total node {len(node['node_name'])}")
    log.info(f"total alias node {len(alias_node['node_name'])}")
    log.info(f"total relation {len(relation['node_id'])}")
    log.info(f"total coding protein relation {len(coding_protein['node_name'])}")

    concept_path = "./results/hgnc/concept"
    os.makedirs(concept_path, exist_ok=True)
    pd.DataFrame(node).to_csv(concept_path + "/concept.csv", index=False)

    synonym_path = "./results/hgnc/synonym"
    os.makedirs(synonym_path, exist_ok=True)
    pd.DataFrame(alias_node).to_csv(synonym_path + f"/concept.csv", index=False)

    relation_path = "./results/hgnc/relation"
    os.makedirs(relation_path, exist_ok=True)
    pd.DataFrame(relation).to_csv(relation_path + f"/relation.csv", index=False)

    gene_coding_protein_path = "./results/hgnc/gene_coding_protein"
    os.makedirs(gene_coding_protein_path, exist_ok=True)
    pd.DataFrame(coding_protein).to_csv(gene_coding_protein_path + f"/relation_raw.csv",
                                        index=False)


if __name__ == "__main__":
    hgnc()
