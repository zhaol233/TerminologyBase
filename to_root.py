# -*- coding: utf-8 -*-
# @Time    : 2022/9/1 15:43
# @Author  : zhaoliang
# @Description: TODO
import pandas as pd

path = "./results/NCI/covid/covid_subnode.csv"

df = pd.read_csv(path,sep=',')
df['tag'].split('|')[1]
