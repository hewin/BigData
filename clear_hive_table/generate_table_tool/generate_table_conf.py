# -*- coding: utf-8 -*-  
import datetime
import sys
import json
import os

if __name__ == "__main__":
    original_conf_file=sys.argv[1]
    target_dir=sys.argv[2]

    original_conf=open(original_conf_file)
    dict={}
    dict["owner"]="chenjianjun@ishumei.com"
    dict["if_drop_table"]="false"
    dict["if_drop_partition"]="true"
    dict["if_delete_hdfs"]="true"
    dict["date_partition_name"]="dt"
    dict["date_partition_format"]="%Y%m%d"
    for line in original_conf:
        line_info=line.strip()
        print(len(line_info))
        if line_info != "" and line_info !=None and len(line_info.split(" "))==4:
            conf_list=line_info.split(" ")
            dict["hive_database"]=conf_list[0]
            dict["hive_table"]=conf_list[1]
            dict["hive_table_location"]=conf_list[2]
            dict["save_window"]=int(conf_list[3])
            print(dict)
            with open("/".join([target_dir,".".join([conf_list[0],conf_list[1],"json"])]),"w") as write:
                json.dump(dict,write,indent=4)
