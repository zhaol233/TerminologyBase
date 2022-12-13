# -*- coding: utf-8 -*-
# @Time    : 2022/4/21 17:06
# @Author  : zhaoliang
# @Description: TODO


import logging

root_path = 'D:/python_ws/term'

verhoeff_table_d = (
    (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    (1, 2, 3, 4, 0, 6, 7, 8, 9, 5),
    (2, 3, 4, 0, 1, 7, 8, 9, 5, 6),
    (3, 4, 0, 1, 2, 8, 9, 5, 6, 7),
    (4, 0, 1, 2, 3, 9, 5, 6, 7, 8),
    (5, 9, 8, 7, 6, 0, 4, 3, 2, 1),
    (6, 5, 9, 8, 7, 1, 0, 4, 3, 2),
    (7, 6, 5, 9, 8, 2, 1, 0, 4, 3),
    (8, 7, 6, 5, 9, 3, 2, 1, 0, 4),
    (9, 8, 7, 6, 5, 4, 3, 2, 1, 0))
verhoeff_table_p = (
    (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    (1, 5, 7, 6, 2, 8, 3, 0, 9, 4),
    (5, 8, 0, 3, 7, 9, 6, 1, 4, 2),
    (8, 9, 1, 6, 0, 4, 3, 5, 2, 7),
    (9, 4, 5, 3, 1, 2, 6, 8, 7, 0),
    (4, 2, 8, 6, 5, 7, 3, 9, 0, 1),
    (2, 7, 9, 3, 8, 0, 6, 4, 1, 5),
    (7, 0, 4, 6, 9, 1, 3, 2, 5, 8))
verhoeff_table_inv = (0, 4, 3, 2, 1, 5, 6, 7, 8, 9)

base_node = {"node_id": [], "node_name": [], "tag": [], "source": [], "original_code": []}
base_relation = {"relation_id": [], "node_id": [], "relationed_node_id": [], "relation_tag": [], "source": [],
                 "original_code": []}


def calcsum(number):
    """For a given number returns a Verhoeff checksum digit"""
    c = 0
    for i, item in enumerate(reversed(str(number))):
        c = verhoeff_table_d[c][verhoeff_table_p[(i + 1) % 8][int(item)]]
    return verhoeff_table_inv[c]


def checksum(number):
    """For a given number generates a Verhoeff digit and
    returns number + digit"""
    c = 0
    for i, item in enumerate(reversed(str(number))):
        c = verhoeff_table_d[c][verhoeff_table_p[i % 8][int(item)]]
    return c


def generateVerhoeff(number):
    """For a given number returns number + Verhoeff checksum digit"""
    return "%s%s" % (number, calcsum(number))


def validateVerhoeff(number):
    """Validate Verhoeff checksummed number (checksum is last digit)"""
    return checksum(number) == 0


def generate_id(node_type, db_code, seq):
    """
    Args:
        node_type: 节点类型，概念：C   描述：D   关系：R    同义词：S，
        db_code: 数据库编号，两位
        seq:术语编号，左补0
    Returns:
    """
    code = node_type + db_code.rjust(3, '0') + str(seq).rjust(12, '0')
    return code


# def generate_mapping_id(node_type, db_code, seq):
#     code = node_type + db_code.rjust(3, '0') + '1' + str(seq).rjust(11, '0')
#     return code


def generate_top_id(node_type, db_code, seq):
    code = node_type + db_code.rjust(3, '0') + '11' + str(seq).rjust(10, '0')
    return code


def generate_id_with_origin_code(node_type, db_code, id):
    origin_id = ''
    if id.startswith('C'):
        origin_id = id[1:]
    else:
        origin_id = id.split(':')[1]
    code = node_type + db_code.rjust(3, '0') + origin_id.rjust(12, '0')
    return code


db_code = {
    "mapping": '00',

    "biology_and_medical_terminology": '100',
    "copd": '101',
    'covid19': '102',
    'lung': '103',
    'asthma': '104',

    "HGNC": "09",
    "GO": "10",
    "UniProt": "11",
    "HPO": "12",
    "KEGG": "13",
    "NCI": "14",
    'OMIM': '15',
    'ORPHA': '17',
    "entrez": '16'

}


def add_relation(relation_dict, relation_id, node_id, relationed_node_id, relation_tag, source, original_code=''):
    relation_dict["relation_id"].append(relation_id)
    relation_dict["node_id"].append(node_id)
    relation_dict["relationed_node_id"].append(relationed_node_id)
    relation_dict["relation_tag"].append(relation_tag)
    relation_dict["source"].append(source)
    relation_dict["original_code"].append(original_code)
    return relation_dict


def add_data(datadict, data):
    for key, value in zip(list(datadict.keys()), data):
        datadict[key].append(value)
    return datadict


def add_node(node_dict, node_id, node_name, tag, source, original_code):
    node_dict['node_id'].append(node_id)
    node_dict['node_name'].append(node_name)
    node_dict['tag'].append(tag)
    node_dict['source'].append(source)
    node_dict['original_code'].append(original_code)
    return node_dict


def add_go_protein_relation(relation, node_name, node_id, relation_tag, gene_name, protein_name, protein_id,
                            relationId, source='GO', relation_node_id=""):
    relation["node_name"].append(node_name)
    relation["node_id"].append(node_id)
    relation["relation_tag"].append(relation_tag)
    relation["gene_name"].append(gene_name)
    relation["protein_name"].append(protein_name)
    relation["protein_id"].append(protein_id)
    relation["relation_node_id"].append(relation_node_id)
    relation["relation_id"].append(relationId)
    relation["source"].append(source)

    return relation


class MyLogging:
    # 初始化日志
    def __init__(self, log_path, log_level='debug'):
        self.logger = logging.getLogger("my_logger")

        self.level = logging.DEBUG
        if log_level == 'info':
            self.level = logging.INFO

        self.logger.setLevel(self.level)

        # logging.basicConfig(level=logging.DEBUG,
        #                     format='[%(asctime)s] [%(filename)s]-->[%(levelname)s] %(message)s',
        #                     datefmt='%Y-%m-%d %H:%M:%S',
        #                     filename=log_path,
        #                     filemode='a')

        console_format = logging.Formatter(
            fmt='[%(asctime)s] [%(filename)s]-->[%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')

        # filehandler = logging.FileHandler(log_path)
        # filehandler.setLevel(self.level)
        # filehandler.setFormatter(file_format)

        streamhandler = logging.StreamHandler()
        streamhandler.setLevel(logging.INFO)
        streamhandler.setFormatter(console_format)

        # self.logger.addHandler(filehandler)
        self.logger.addHandler(streamhandler)

        self.logger.info("------logger inited-------")

    def get_logger(self):
        return self.logger


log_path = "./run.log"
log = MyLogging(log_path).get_logger()
