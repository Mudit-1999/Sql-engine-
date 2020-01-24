import operator
import sys
import os
import csv
import sqlparse
from sqlparse import sql
from sqlparse import tokens
from collections import defaultdict
from statistics import mean


DISTINCT_FLAG=False
WHERE_FLAG=False
FROM_FLAG=False
SELECT_FLAG=False
STAR_FLAG=False
AGG=False
JOIN_FLAG=False
JOIN_COL= set()


AGGREGATE_OPS = {
    'MAX':max,
    'AVG':mean,
    'MIN':min,
    'SUM':sum
}

com_operator = {
    '>=': operator.ge,
    '<': operator.lt,
    '=': operator.eq,
    '!=': operator.ne,
    '<=': operator.le,
    '>': operator.gt
} 

def fun_error(error):
    print("ERROR: ",error)
    sys.exit()

def validate_query(query):
    try:
        Dlf=False
        query=query.split()
        if len(query) <4:
            fun_error("INVALID SYNTAX")
        if query[0].upper() != "SELECT":
            fun_error("Syntax Mistake:SELECT")

        if query[2].upper() != "FROM" and query[3].upper() != "FROM":
            fun_error("Syntax Mistake:FROM")

        ind = query.index("FROM")
        if ind==3 and query[1].upper() != "DISTINCT":
            fun_error("Syntax Mistake:DISTINCT")
        if ind==3 and query[1].upper() == "DISTINCT":
            Dlf=True

        if Dlf and len(query) >= 6 and query[5].upper() != "WHERE":
            fun_error("Syntax Mistake:WHERE")
        if Dlf==False and  len(query) >= 5 and query[4].upper() != "WHERE":
            fun_error("Syntax Mistake:WHERE")
        return 1
    except:
        print("Something went wrong")
        print("Bye!!!")
        sys.exit()

def row_join(output_table,output_uniq_attri,output_attri,database_info,name):
    try:
        joined_table=[]
        if(len(output_attri)==0):
            return database_info[name]['table'],database_info[name]['unique_attri'],database_info[name]['attri']
        for d1 in output_table:
            for d2 in database_info[name]['table']:
                joined_table.append(d1+d2)
        joined_uniq_attri=output_uniq_attri+database_info[name]['unique_attri']
        joined_attri=output_attri+database_info[name]['attri']
        return joined_table,joined_uniq_attri,joined_attri
    except:
        print("Something went wrong")
        print("Bye!!!")
        sys.exit()

def extract_attri(tok,attri_table):
    try:
        global AGG 
        if isinstance(tok, sql.Identifier):
            attri_table.append(tok)
        if isinstance(tok, sql.Function):
            AGG=True    
            # print((tok.tokens[0], tok.tokens[1].tokens[1]))
            attri_table.append((tok.tokens[0], tok.tokens[1].tokens[1]))
    except:
        print("Something went wrong")
        print("Bye!!!")
        sys.exit()

def parsing(query):
    try:
        global FROM_FLAG,WHERE_FLAG,DISTINCT_FLAG,SELECT_FLAG,STAR_FLAG
        query_info= defaultdict()
        # print("Parsing ")
        attri_table=[]
        query_table=[]
        query_cond=[]
        query_log=[]
        parsed = sqlparse.parse(query)[0]
        for i,tok in enumerate(parsed.tokens):
            # print(i,tok)
            if tok.ttype is tokens.DML and tok.value.upper() == 'SELECT':
                SELECT_FLAG = True

            if tok.ttype is tokens.Keyword and tok.value.upper() == 'FROM':
                FROM_FLAG = True

            # query tables Names
            if FROM_FLAG==False:
                if isinstance(tok, sql.IdentifierList):
                    for col in tok.get_identifiers():
                        extract_attri(col,attri_table)
                else:
                    # DISTINCT
                    if tok.ttype is tokens.Keyword: 
                        DISTINCT_FLAG = True
                    # checking  *
                    elif tok.ttype is tokens.Wildcard:  
                        STAR_FLAG = True
                    else:
                        extract_attri(tok,attri_table)
            # attribute names
            else:
                if isinstance(tok, sql.IdentifierList):
                    for name in tok.get_identifiers():
                        query_table.append(name.value)
                if isinstance(tok, sql.Identifier):
                    query_table.append(tok.value)

            # WHERE
            if isinstance(tok, sql.Where):  
                WHERE_FLAG=True
                for w1 in tok.tokens:
                    # print(w1)
                    if isinstance(w1, sql.Comparison):  
                        # print("1",w1)
                        query_cond.append(w1)
                    elif w1.ttype ==tokens.Whitespace or w1.ttype ==tokens.Punctuation or w1.value.upper()=="WHERE":
                        # print("3",w1.value)
                        pass
                    elif w1.ttype is tokens.Keyword and w1.value.upper()!="WHERE":
                        query_log.append(w1.value.upper())
                    else:
                        # print("4",w1.value)
                        fun_error("SYNTAX ERROR:INCOMPLETE CONDITION")
        
        query_info['table']=query_table        
        query_info['attri']=attri_table
        query_info['cond']=query_cond
        query_info['log']=query_log
        return query_info
    except:
        print("Something went wrong")
        print("Bye!!!")
        sys.exit()

def where_condition(query_info,output_table,output_attri,output_uniq_attri):
    try:
        global JOIN_FLAG,JOIN_COL
        data_x=[]
        data_z=[]
        data_t=[]
        temp=[]
        # print("WHERE")
        for i,condition in enumerate(query_info['cond']):
            # print(condition)
            for t1 in condition:
                # print("hi",t1.value)
                if not t1.ttype:
                    data_t.append('A')
                    res_list = [i for i in range(len(output_attri)) if output_attri[i] == t1.value] 
                    res_list += [i for i in range(len(output_attri)) if output_uniq_attri[i] == t1.value] 
                    # print(t1.value,res_list)
                    if len(res_list)==0 :
                        fun_error("Attribute not found")
                    elif len(res_list)>=2 :
                        fun_error("Ambiguous Attribute")
                    data_x.append(res_list[0])
                    data_z.append(t1.value)
                elif t1.value !=" " and t1.ttype == tokens.Comparison:
                    data_x.append(t1.value)
                    data_z.append(t1.value)
                    data_t.append("C")
                elif t1.value !=" ":
                    # print(" eef",float(t1.value))
                    data_x.append(float(t1.value))
                    data_t.append("I")
        
        # print(data_x,data_t)
        if len(data_x)%3!=0 or len(data_x)==0:
            fun_error("INVALID QUERY")
        if len(data_x)<=3 and len(query_info['log'])!=0:
            fun_error("INVALID QUERY")
        if len(data_x)>3 and len(query_info['log'])==0:
            fun_error("INVALID QUERY")
        for j,row in enumerate(output_table):
            res=[]
            i=0
            while(i<len(data_x)):
                var1 = int(data_x[i]) if data_t[i] != "C" else data_x[i]
                i+=1
                var2 = int(data_x[i]) if data_t[i] != "C" else data_x[i]
                i+=1
                var3 = int(data_x[i]) if data_t[i] != "C" else data_x[i]
                i+=1
                if data_t[i-3] == "A" and data_t[i-1]=="A" and data_t[i-2]=="C" and var2=="=":
                    JOIN_FLAG=True
                    # JOIN_COL.add(data_z[i-1])
                    JOIN_COL.add(output_uniq_attri[ data_x[i-1]])
                    JOIN_COL.add(output_uniq_attri[ data_x[i-3]])
                    JOIN_COL.add(output_attri[data_x[i-1]])
                    JOIN_COL.add(output_attri[data_x[i-3]])
                # print(var1,var2,var3)
                temp1=row[var1] if data_t[i-3]=="A" else var1
                temp3=row[var3] if data_t[i-1]=="A" else var3
                res.append((com_operator[var2](temp1,temp3)))
            if len(res)>1:
                if query_info['log']==['AND']:
                    if all(res) ==True:
                        temp.append(row)
                else:
                    if any(res)==True:
                        temp.append(row)
            else:
                if res[0]==True:
                    temp.append(row)
        return temp

    except:
        print("Something went wrong")
        print("Bye!!!")
        sys.exit()

def print_output(query_info,output_table,output_uniq_attri,output_attri):
    try:
        if STAR_FLAG == False:
            temp_attri=[]
            for i,col_name in enumerate(query_info['attri']):
                # print(JOIN_COL)
                if JOIN_FLAG  and col_name.value in JOIN_COL:
                    # print(col_name)
                    continue
                res_list = [i for i in range(len(output_attri)) if output_attri[i] == col_name.value] 
                res_list += [i for i in range(len(output_attri)) if output_uniq_attri[i] == col_name.value] 
                # print(col_name,res_list)
                if(len(res_list)==0):
                    fun_error("Attribute not found")
                elif(len(res_list)>=2):
                    fun_error("Ambiguous Attribute")
                else:
                    temp_attri.append(res_list[0])
            if JOIN_FLAG:
                ele=JOIN_COL.pop()
                res_list = [i for i in range(len(output_attri)) if output_attri[i] == ele ] 
                res_list += [i for i in range(len(output_attri)) if output_uniq_attri[i] == ele]
                temp_attri.append(res_list[0])
            if DISTINCT_FLAG:
                myset=set()
                for rows in output_table:
                    myset.add(tuple(rows[number] for number in temp_attri))
                for number in temp_attri:
                    print(output_uniq_attri[number],end=" ")
                print()
                for rows in myset:
                    for k in rows:
                        print(k,end=" ")
                    print()
            else:
                for number in temp_attri:
                    print(output_uniq_attri[number],end=" ")
                print()
                for rows in output_table:
                    for number in temp_attri:
                        print(rows[number],end=" ")
                    print()
        else:
            uniques_number = set()
            for col_name in output_uniq_attri:
                if JOIN_FLAG  and col_name in JOIN_COL:
                    continue
                res_list = [i for i in range(len(output_attri)) if output_uniq_attri[i] == col_name]
                uniques_number.add(res_list[0])

            if JOIN_FLAG:
                ele=JOIN_COL.pop()
                res_list = [i for i in range(len(output_attri)) if output_attri[i] == ele] 
                res_list += [i for i in range(len(output_attri)) if output_uniq_attri[i] == ele]
                uniques_number.add(res_list[0])

            if DISTINCT_FLAG:
                myset=set()
                for rows in output_table:
                    myset.add(tuple(rows[number] for number in uniques_number))
                for number in uniques_number:
                    print(output_uniq_attri[number],end=" ")
                print()
                for rows in myset:
                    for k in rows:
                        print(k,end=" ")
                    print()
            else:
                for number in uniques_number:
                    print(output_uniq_attri[number],end=" ")
                print()
                for rows in output_table:
                    for number in uniques_number:
                        print(rows[number],end=" ")
                    print()
    except:
        print("Something went wrong")
        print("Bye!!!")
        sys.exit()


def process_query(query_info,database_info):
    try:
        global FROM_FLAG,WHERE_FLAG,DISTINCT_FLAG,SELECT_FLAG,STAR_FLAG
        table_name= query_info['table']
        result=[]
        result_attri=[]
        temp_attri=[]
        if len(table_name) > 0:
            output_attri=[]
            output_uniq_attri=[]
            output_table=[]
            for i,name in enumerate(table_name):
                if name not in table_name_list:
                    fun_error("Table " + name +" not found.")
                output_table,output_uniq_attri,output_attri = row_join(output_table,output_uniq_attri,output_attri,database_info,name)  
            # print("join_table",output_attri,output_table)
        else:
            fun_error("TABLE NOT GIVEN")
        # For where querry
        # print(WHERE_FLAG)
        if WHERE_FLAG:
            output_table=where_condition(query_info,output_table,output_attri,output_uniq_attri)
        # print(output_table)

        if AGG and len(query_info['attri'])>=2:
            fun_error("NOT SUPPORTED")
        if AGG:
            temp=[]
            agg_op=None
            for i,atr in enumerate(query_info['attri']):
                agg_op=atr[0].value
                col_name=atr[1].value
                res_list = [i for i in range(len(output_attri)) if output_attri[i] == col_name] 
                res_list += [i for i in range(len(output_attri)) if output_uniq_attri[i] == col_name] 
                if len(res_list)==0:
                    fun_error("Attribute not found")
                elif len(res_list)>=2:
                    fun_error("Ambiguous Attribute")
                # print(agg_op,col_name,res_list)
                for j,row in enumerate(output_table):
                    temp.append(row[res_list[0]])
            if len(temp)!=0:
                print(agg_op,"(",col_name,")",AGGREGATE_OPS[agg_op](temp))
            else:
                print(" ")
            return 0
        else:
            print_output(query_info,output_table,output_uniq_attri,output_attri)
    except:
        print("Something went wrong")
        print("Bye!!!")
        sys.exit()


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
    process_query(query_info,database_info)

