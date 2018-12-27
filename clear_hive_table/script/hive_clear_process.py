# -*- coding: utf-8 -*-  
import datetime
import sys
import json
import os

class TableConstant:
    """docstring for TableConstant"""
    def __init__(self, arg):
        try:
            conf_file_reader=open(arg)
            self.conf_dict=json.load(conf_file_reader)
            conf_dict_sample={
               "hive_database":"hive_database",
               "hive_table":"hive_table",
               "hive_table_location":"hive_table_location",
               "owner":"owner",
               "if_drop_table":"if_drop_table",
               "if_drop_partition":"if_drop_partition",
               "if_delete_hdfs":"if_delete_hdfs",
               "save_window":"save_window",
               "date_partition_name":"date_partition_name",
               "date_partition_format":"date_partition_format"
            }
            for key in conf_dict_sample.keys():
                if key not in self.conf_dict :
                    raise Exception("The key(%s) not in conf_file(%s) Error!"%(key))
            conf_file_reader.close()
        except Exception:
            raise Exception("Read conf_file(%s) Error!"%(conf_file))
        else:
            pass
        finally:
            pass
    def get(self,key):
        return self.conf_dict[key] if key in self.conf_dict else None

def get_date_before(now_date,date_sub,input_format,result_format):
    import datetime
    import time
    now_time=datetime.datetime.strptime(now_date,input_format).timestamp()
    now_time-=86400*date_sub
    target_date=time.strftime(result_format,time.localtime(now_time))
    return target_date

if __name__ == "__main__":
    conf_file=sys.argv[1]
    is_partition_table=sys.argv[2]
    date=sys.argv[3]
    hive_database="hive_database"
    hive_table="hive_table"
    hive_table_location="hive_table_location"
    if_drop_table="if_drop_table"
    if_drop_partition="if_drop_partition"
    if_delete_hdfs="if_delete_hdfs"
    date_partition_name="date_partition_name"
    date_partition_format="date_partition_format"
    save_window="save_window"

    table_constant = TableConstant(arg=conf_file)
    table_name='.'.join([table_constant.get(hive_database),table_constant.get(hive_table)])
    date_before=get_date_before(str(date),int(table_constant.get(save_window)),'%Y%m%d',table_constant.get(date_partition_format))
    date_partition_name=table_constant.get(date_partition_name)
    hive_table_location=table_constant.get(hive_table_location)
    date_partition_format=table_constant.get(date_partition_format)
    if_delete_hdfs=table_constant.get(if_delete_hdfs)
    if_drop_partition=table_constant.get(if_drop_partition)
    if_drop_table=table_constant.get(if_drop_table)

    #Step1 删除hdfs
    if if_delete_hdfs == "true":
        if int(is_partition_table)==0:
            partition_hdfs="%s"(hive_table_location)
            drop_table_script="hadoop fs -test -e '%s'; if [ $? -eq 0 ];then hadoop fs -rm -r -skipTrash %s; fi"%(partition_hdfs)
        else:
            partition_hdfs="%s/%s=%s"%(hive_table_location,date_partition_name,date_before)
            drop_table_script="hadoop fs -test -e '%s'; if [ $? -eq 0 ];then hadoop fs -rm -r -skipTrash %s; fi"%(partition_hdfs,partition_hdfs)
        result=os.system(drop_table_script)
        print(drop_table_script+("success!" if result==0 else "failed!")+"\n")
    elif if_delete_hdfs == "false":
        pass
    else:
        raise Exception("Parameter if_delete_hdfs(%s) Error!"%(if_delete_hdfs))

    #Step2 删除分区
    if if_drop_partition == "true":
        if int(is_partition_table)>0:
            drop_partition_script="hive -e 'ALTER TABLE %s DROP IF EXISTS PARTITION(%s=%s)'"%(table_name,date_partition_name,date_before)
            result=os.system(drop_partition_script)
            print(drop_partition_script+("success!" if result==0 else "failed!")+"\n")
        else:
            raise Exception("Parameter if_drop_partition(%s) Error,%s is not a table defined with partitions!"%(if_drop_partition,table_name))
    elif if_drop_partition == "false":
        pass
    else:
        raise Exception("Parameter if_drop_partition(%s) Error!"%(if_drop_partition))

    #Step3 删除表
    if if_drop_table == "true":
        drop_table_script="hive -e 'DROP TABLE IF EXISTS %s'"%(table_name)
        result=os.system(drop_table_script)
        print(drop_table_script+("success!" if result==0 else "failed!")+"\n")
    elif if_drop_table == "false":
        pass
    else:
        raise Exception("Parameter if_drop_table(%s) Error!"%(if_drop_table))