# -*- coding: utf-8 -*-
# @Time    : 2022/9/15 16:26
# @Author  : zhaoliang
# @Description: TODO
import os

import pandas as pd
from term.src.utils.util import add_data, db_code, generate_id

biology_and_medical_terminology_base_concept = pd.read_csv(
    "../../results/biology_and_medical_terminology_base/concept.csv")

relation = {"relation_id": [], "node_id": [], 'node_source': [], "relationed_node_id": [], "relationed_node_source": [],
            "relation_tag": [], "source": [], "original_code": []}

rel_sep_id = 1
relation_id = generate_id('R', db_code['biology_and_medical_terminology'], rel_sep_id)
rel_sep_id += 1

# HGNC to genetic material
relation = add_data(relation, [relation_id, 'C009000000000000', 'HGNC', '21', "Biology and Medical Terminology Base",
                               'is_A', "Biology and Medical Terminology Base", ''])

# UniProt to Biological substance
relation_id = generate_id('R', db_code['biology_and_medical_terminology'], rel_sep_id)
rel_sep_id += 1
relation = add_data(relation,
                    [relation_id, 'C01100000000000000', 'UniProt', '22', "Biology and Medical Terminology Base",
                     'is_A', "Biology and Medical Terminology Base", ''])

# GO to Functional genome
relation_id = generate_id('R', db_code['biology_and_medical_terminology'], rel_sep_id)
rel_sep_id += 1
relation = add_data(relation, [relation_id, 'C010000000008150', 'GO', '23', "Biology and Medical Terminology Base",
                               'is_A', "Biology and Medical Terminology Base", ''])
relation_id = generate_id('R', db_code['biology_and_medical_terminology'], rel_sep_id)
rel_sep_id += 1
relation = add_data(relation, [relation_id, 'C010000000003674', 'GO', '23', "Biology and Medical Terminology Base",
                               'is_A', "Biology and Medical Terminology Base", ''])
relation_id = generate_id('R', db_code['biology_and_medical_terminology'], rel_sep_id)
rel_sep_id += 1
relation = add_data(relation, [relation_id, 'C010000000005575', 'GO', '23', "Biology and Medical Terminology Base",
                               'is_A', "Biology and Medical Terminology Base", ''])

# NCI activity to activity
relation_id = generate_id('R', db_code['biology_and_medical_terminology'], rel_sep_id)
rel_sep_id += 1
relation = add_data(relation, [relation_id, 'C014000000043431', 'NCI', '24', "Biology and Medical Terminology Base",
                               'is_A', "Biology and Medical Terminology Base", ''])

# to Abnormal
# HPO
relation_id = generate_id('R', db_code['biology_and_medical_terminology'], rel_sep_id)
rel_sep_id += 1
relation = add_data(relation, [relation_id, 'C012000000000001', 'HPO', '25', "Biology and Medical Terminology Base",
                               'is_A', "Biology and Medical Terminology Base", ''])
# NCI molecular abnormal
relation_id = generate_id('R', db_code['biology_and_medical_terminology'], rel_sep_id)
rel_sep_id += 1
relation = add_data(relation, [relation_id, 'C014000000003910', 'NCI', '25', "Biology and Medical Terminology Base",
                               'is_A', "Biology and Medical Terminology Base", ''])
# NCI abnormal cell
relation_id = generate_id('R', db_code['biology_and_medical_terminology'], rel_sep_id)
rel_sep_id += 1
relation = add_data(relation, [relation_id, 'C014000000012913', 'NCI', '25', "Biology and Medical Terminology Base",
                               'is_A', "Biology and Medical Terminology Base", ''])

# NCI bio
relation_id = generate_id('R', db_code['biology_and_medical_terminology'], rel_sep_id)
rel_sep_id += 1
relation = add_data(relation, [relation_id, 'C014000000012913', 'NCI', '25', "Biology and Medical Terminology Base",
                               'is_A', "Biology and Medical Terminology Base", ''])

relation_path = "../../results/merge_db/Biology_and_Medical_Terminology_Base/relation"
os.makedirs(relation_path, exist_ok=True)
pd.DataFrame(relation).to_csv(relation_path + "/relation.csv", index=False)
