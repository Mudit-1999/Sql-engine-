import operator
import sys
import os
import csv
import sqlparse
from sqlparse import sql
from sqlparse import tokens
from collections import defaultdict

def read_table(loc_file,file_name,database_info):
    try:
        path_name=loc_file + file_name.strip('\n') + ".csv"
        with open(path_name,'r') as table:
            table_data=table.readlines()
        file_name=file_name.upper()
        if len(database_info[file_name]['attri']) != len(table_data[0].split(',')):
            print("update Metadata")
        else:
        # validate_query(query)
            temp=[]
            for data in table_data:
                # only int data type
                temp.append([int(i) for i in data.split(',')])
        return temp
    except:
        print("Something went wrong")
        print("Bye!!!")
        sys.exit()

def read_meta_data(loc_file,database_info):
    try:
        with open(loc_file,'r') as fo:
            lines=fo.readlines()
        table_name_list=[]
        original =[]
        for word_number in range(len(lines)):
            if "<begin_table>" in lines[word_number]:
                word_number+=1
                table_name=lines[word_number].strip('\n')
                original.append(table_name)
                table_name=table_name.upper()
                database_info[table_name]={}
                table_name_list.append(table_name)
                database_info[table_name]['unique_attri']=[]
                database_info[table_name]['attri']=[]
                word_number+=1
                while "<end_table>" not in lines[word_number]:
                    database_info[table_name]['unique_attri'].append(table_name + '.'+ lines[word_number].strip('\n'))
                    database_info[table_name]['attri'].append(lines[word_number].strip('\n'))
                    word_number+=1
        return table_name_list,original
    except:
        print("Something went wrong")
        print("Bye!!!")
        sys.exit()




if __name__ == "__main__":
    # Checking format
    if len(sys.argv) != 2:
        print("[Usage]")
        print("Format: python3 [Filename.py] [Single Querry in Quotes]")
    
    # Reading MetaData
    LOCATION_FILE = "./files/metadata.txt"
    database_info={}
    table_name_list,original = read_meta_data(LOCATION_FILE,database_info)
    
    # Reading Table Data
    for i,name in enumerate(original):
        LOCATION_FILE = "./files/"
        database_info[name.upper()]['name']=name.upper()
        database_info[name.upper()]['table']= read_table(LOCATION_FILE,name,database_info)
    # print(database_info)

     
    # reading Querry 
    query=sys.argv[1].upper().strip()
    if query[-1] !=';':
        fun_error("Missing semicolon")
    validate_query(query[:-1])
    query_info= parsing(query)
    # print(query_info)