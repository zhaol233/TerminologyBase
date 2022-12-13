# -*- coding: utf-8 -*-
# @Time    : 2022/9/14 21:00
# @Author  : zhaoliang
# @Description: TODO
import os

import pandas as pd

threshold = 40000

type2file = {"concept": 'concept.csv', 'synonym': 'concept.csv', 'relation': 'relation.csv'}


def split(path, node_type):
    df = pd.read_csv(path + f'{node_type}/{type2file[node_type]}')

    start = 0
    count = 0

    while start < df.shape[0]:
        res_path = path + f'split/{node_type}_split_{count}/'
        os.makedirs(res_path, exist_ok=True)
        print(df.iloc[start:start + threshold].shape)
        df.iloc[start:start + threshold].to_csv(res_path + type2file[node_type], index=False)
        start += threshold
        count += 1


if __name__ == "__main__":

    split('../results/hgnc/', 'relation')

    # split('./results/go/', 'relation')
    # split('./results/go/', 'synonym')

    # split('./results/uniprot/', 'concept')
    # split('./results/uniprot/', 'relation')

    # split('./results/NCI/', 'concept')
    # split('./results/NCI/', 'synonym')
    # split('../results/NCI/', 'relation')
