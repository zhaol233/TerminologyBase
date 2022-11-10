import pandas as pd
from util import *
import os

path_dict = {
    "hgnc_path": "./hgnc_relation.csv",
    "go_path": "./go_relation.csv",
    "hpo_path": "./hpo_relation.csv",
    "uniprot_path": "./uniprot_relation.csv",
    "nclt_path": "./results/nclt/Neoplastic/nclt_relation.csv",
    "nclt_drug_path": "./results/nclt/drug/nclt_drug_relation.csv",
    "coding_protein_path": "./coding_protein.csv"
}

relation_map = {}
log_path = "./relation_split.log"
log = MyLogging(log_path).get_logger()


def relation_split(file_path, name):
    log.info("relation_file: " + file_path)
    df = pd.read_csv(file_path, header=0)
    log.info(f"total relation: {df.shape[0]}")

    dir_name = os.path.split(file_path)[0]
    dir_name += f"/{name}"
    os.makedirs(dir_name, exist_ok=True)
    relation_tag = df["relation_tag"].drop_duplicates()
    print(list(relation_tag))
    for relation in list(relation_tag):
        df.loc[df['relation_tag'] == relation, :].to_csv(dir_name + f'/{relation}.csv', index=False)
        log.info(f"operate {relation} finish")


def df_relation_split(df, path_name):
    """
    Args:
        df: data Dataframe
        dir_name: result path name

    Returns:
    """
    log.info(f"total relation: {df.shape[0]}")
    dir_name = f"{path_name}"
    os.makedirs(dir_name, exist_ok=True)
    relation_tag = df["relation_tag"].drop_duplicates()
    print(list(relation_tag))
    pd.DataFrame(list(relation_tag)).to_csv(dir_name + '/test.csv', index=False)
    for relation in list(relation_tag):
        df.loc[df['relation_tag'] == relation, :].to_csv(dir_name + f'/{relation}.csv', index=False)


if __name__ == "__main__":
    pass
    # for key, value in path_dict.items():
    #     print(key)
    #     relation_split(value)
    # relation_split(path_dict['nclt_path'])
    relation_split("D:\\python_ws\\术语库\\results\\go\\go_gene_616308.csv", "go_protein")
