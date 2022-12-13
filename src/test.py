import pandas as pd

local_path = "../results/disease/"

path1 = "disease_disorder_finding1/term_19681.csv"
path2 = "disease_disorder_finding1/is_A.csv"
path3 = "disease_disorder_finding1/alias.csv"
path4 = "disease_disorder_finding1/alias_node.csv"

path11 = "disease_disorder_finding/term_21072.csv"
path21 = "disease_disorder_finding/is_A.csv"
path31 = "disease_disorder_finding/alias.csv"
path41 = "disease_disorder_finding/alias_node.csv"

df1 = pd.read_csv(local_path + path1, sep=',')  # term
df1['tag'] = 'NCI|Disease_Disorder_or_Finding|Concept'

df2 = pd.read_csv(local_path + path2, sep=',')  # is_a
df3 = pd.read_csv(local_path + path3, sep=',')  # alias
df4 = pd.read_csv(local_path + path4, sep=',')  # alias node

df11 = pd.read_csv(local_path + path11, sep=',')  # term
df21 = pd.read_csv(local_path + path21, sep=',')  # is_a
df31 = pd.read_csv(local_path + path31, sep=',')  # alias
df41 = pd.read_csv(local_path + path41, sep=',')  # alias node

df_node = pd.concat([df1, df11], axis=0)
df_is_a = pd.concat([df2, df21], axis=0)
df_alias = pd.concat([df3, df31], axis=0)
df_alias_node = pd.concat([df4, df41], axis=0)

df_is_a = df_is_a[df_is_a['relation_node_id'].isin(df_node['node_id'])]

df_node.to_csv('./results/disease/nci_disease.csv', index=False)
df_alias.to_csv('./results/disease/nci_disease_alias.csv', index=False)
df_alias_node.to_csv('./results/disease/nci_disease_alias_node.csv', index=False)
df_is_a.to_csv('./results/disease/nci_disease_is_a.csv', index=False)
