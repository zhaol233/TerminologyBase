import pandas as pd
import os


TYPE = {
    'Concept': 'C',
    'Description': 'D',
    'Synonym': 'S',
    'Relation': 'R'
}

# 对当前目录下的文件进行编码
def encode_import_file(file_path: str, kb_id: int, type: str, current_node_nums=0):
    df = pd.read_csv(file_path, dtype=str)
    data_lines = df.shape[0]
    
    file_name = os.path.basename(file_path).split('.')[0]
    data_store_dir = "encode_" + file_name

    if not os.path.exists(data_store_dir):
        os.mkdir('./' + data_store_dir)

    code_list = [TYPE.get(type) + format(kb_id, "03d") + format(node_num, "012d")
                 for node_num in range(current_node_nums, current_node_nums + data_lines)]
    code_name = TYPE.get(type).lower() + 'id'

    df.insert(df.shape[1], code_name, code_list)

    pd.DataFrame(df).to_csv(data_store_dir + '/' + file_name + ".csv", index=False)


if __name__ == "__main__":
    encode_import_file('./term.csv', 6, 'Concept', 89710)
