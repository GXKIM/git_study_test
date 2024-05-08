####just test###

import json
import pandas as pd
from collections import deque
import random
import re
import regex

SRM = []
def transfer_data():

    df = pd.read_csv("milvus_query_result.csv")
    new_df = df[["content","tableinfo","sql"]]
    print(new_df)

    # new_df.to_excel("output1.xlsx", index=False)
    return new_df

def data_processing():

    sub_data = transfer_data()
    sub_data_len = len(sub_data)
    print()
    for i in range(sub_data_len):
        mode = {
                "id": f"identity_{i}",
                "conversations": [
                {
                "role": "user",
                "content": f"""下列问题都是关于mysql的问题,请你帮助我回答下列问题,根据表信息及问题，输出对应的mysql查询语句
###表信息如下：
{sub_data["tableinfo"][i]}
2.
请按以下规则分析:  
      
只输出SQL语句
###指令: {sub_data["content"][i]}
###输出:"""
                },
                {
                    "role": "assistant",
                    "content": f"""{sub_data["sql"][i]}""",
                }
                ]
                }
        SRM.append(mode)
    return SRM

def end_of_semicolon(text):
    # 统一处理sql尾部分号问题，针对没有分号和space+分号
    if text[-1] != ';':
        text = text + ';'
    text = re.sub(r'\s*;+$', ';', text)
    return text

def wrap_to_space(text):
    # 将\n替换为space
    normalized_text = text.replace('\n',' ')
    normalized_text = re.sub(r'\s+', ' ', normalized_text)
    return normalized_text

def uniform_space(text):
    # 使用正则表达式将所有Unicode空格编码统一为普通空格
    normalized_text = regex.sub(r'\p{Zs}', ' ', text)
    return normalized_text

with open('../training_srm/text2sql.json', 'r', encoding='utf-8') as file:
    item = json.load(file)
    dataset_len = len(data_processing())
    print(dataset_len)
    sql_len = 3000 - dataset_len
    random_item = deque(random.choices(item,k=sql_len))
    # print(len(random_item))
    n = 0
    while random_item:

        current = random_item.popleft()
        INPUT = current['instruction']+current['input']
        OUTPUT = end_of_semicolon(wrap_to_space(uniform_space(current['output'])))

        if 'company_name' in INPUT.lower() or 'company_name' in OUTPUT.lower():
            random_item.append(random.choice(item))
        else:
            mode = {
                    "id": f"identity_{dataset_len+n}",
                    "conversations": [
                    {
                        "role": "user",
                        "content": INPUT
                    },
                    {
                        "role": "assistant",
                        "content": OUTPUT
                    }
                    ]
                    }
            SRM.append(mode)
        n += 1
#
# print(len(SRM))
with open('SUB_data_0508.json', 'w', encoding='utf-8') as file:
    json.dump(SRM,file,indent=4, ensure_ascii=False)
# print(len(data_processing()))
# print(data_processing()[:5])