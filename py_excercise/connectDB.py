import sys
import psycopg2
import pandas as pd
import numpy as np
import separator as sp
from pathlib import Path
import shutil

sql_query_data_count = "SELECT schemaname as schema_name,relname as table_name,n_live_tup as count " \
                       "FROM pg_stat_user_tables ORDER BY n_live_tup DESC"
filename = "C:\Tools\programing\python\\temp\output.xlsx"

"""
# get column quantity from matched schema & table name
"""
def get_size(param_excel_row, rows):
    for row in rows:
        if param_excel_row.iat[0,0] == row[0] and param_excel_row.iat[0,1] == row[1]:
            return row[2]

    return None

'''
get table count from db
'''
def get_table_count(param_excel, param_rs_sizes):
    target_index_count = param_excel.shape[0]
    for index in range(0, target_index_count):
        excel_row = param_excel[index:index + 1]

        table_count = None
        for row in param_rs_sizes:
            if excel_row.iat[0, 0] == row[0] and excel_row.iat[0, 1] == row[1]:
                table_count = row[2]
                break
        if not table_count is None:
            param_excel.at[index, 'size'] = table_count

    return param_excel

#connect to DB
try:
    conn = psycopg2.connect(database="firstdb", user="jerry", password="zaq12wsx", host="localhost", port="5432")
except Exception(BaseException) :
    print("failed to connect to DB.")
    sys.exit(1)

print("Opened database successfully")

# fetch data from DB
cursor = conn.cursor()
cursor.execute(sql_query_data_count)
result_sizes = cursor.fetchall()
# for row in table_sizes:
#     print(row)
# print("------------------- print table columns ----------------")
# description = cursor.description
# column_list = [column[0] for column in description]
# print(column_list)

# read excel
print("\n---------- read target objects from excel -------------")
excel = pd.read_excel(filename, sheet_name="Sheet1");
# excel['size']=None
# result_df = pd.DataFrame(index=[],columns=excel.columns)

print("\n------------------- print excel filled with count ----------------")
# excel = get_table_count(excel,result_sizes)

sorted_tables = excel.sort_values(by=['type','size'], ascending=True)
print(sorted_tables)

# delete root directory if exist
root = 'dbmg'
root_dir = Path(root)
if root_dir.exists():
    shutil.rmtree(root)
root_dir.mkdir(parents=True, exist_ok=True)
root_dir = root_dir.name

# filter data by type
print("\n------------------- print data filtered by type ----------------")
group_num = 3
type_num = 3
sorted_tables['group']=group_num
col_name = 'size'
filename_suffix = '_tablelist.csv'

for i_type in range(1,type_num+1):
    # print("________________", str(i_type), "________________")
    df_every_group = sorted_tables.query('type==' + str(i_type))
    df_every_group = sp.separate_to_groups(df_every_group, col_name, group_num)
    df_every_group = df_every_group[['schema', 'group', 'table_name', 'type','size','date']]
    print(df_every_group)

    group_indices = df_every_group.groupby('group').count().index.values
    for group_index in group_indices:
        dir_name = root_dir + '/mg' + str(group_index)
        directory = Path(dir_name)
        directory.mkdir(parents=True, exist_ok=True)

        schema_indices = df_every_group.query('group=='+str(group_index)).groupby('schema').count().index.values
        for schema_index in schema_indices:
            filename = dir_name + '/' + schema_index + filename_suffix
            tmp_write_to = df_every_group.query('group=='+str(group_index)+' and schema=="'+schema_index+'"')
            tmp_write_to.to_csv(filename, index=False,header=False,mode='a+')

conn.close()
