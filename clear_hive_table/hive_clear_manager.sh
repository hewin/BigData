#!/bin/bash

source ~/.bashrc
export PATH=/opt/anaconda3/bin:$PATH
export PYSPARK_PYTHON="/opt/anaconda3/bin/python"
export SPARK_HOME=/opt/spark-2.1.1-bin-hadoop2.6
export SPARK_CONF_DIR=$SPARK_HOME/conf
export PYLIB=$SPARK_HOME/python/lib
export PYTHONPATH="$PYLIB:$PYLIB/py4j-0.10.4-src.zip:$PYLIB/pyspark.zip"
export PATH=$SPARK_HOME/bin:$PATH

BIN_DIR=$(cd `dirname $0`;pwd)
export script_dir=${BIN_DIR}/script
export log_dir=${BIN_DIR}/logs
export table_conf_dir=${BIN_DIR}/tables_conf
date=$1

if [ ! -d ${log_dir} ];
then
  mkdir ${log_dir}
fi

log_file=${log_dir}/${date}.log
echo "[`date`,INFO] hive_clear_manager.sh of peroid ${date} Start!"  > ${log_file}
for file in `ls ${table_conf_dir}`
do
    echo $file
    task_file=${table_conf_dir}/${file}
    echo "[`date`,INFO] ${file} Start!" >> ${log_file}
    #1.预处理
    #判断是否是分区表
    JQ_EXEC=`which jq`
    hive_database=$(cat ${task_file} | ${JQ_EXEC} .hive_database | sed 's/\"//g')
    hive_table=$(cat ${task_file} | ${JQ_EXEC} .hive_table | sed 's/\"//g')
    hivetable=${hive_database}"."${hive_table}
    is_partition_table=`hive -e "desc formatted ${hivetable}" | grep -i "partition" | wc -l`
  
    #2.处理
    python ${script_dir}/hive_clear_process.py ${task_file} ${is_partition_table} ${date} >> ${log_file}
    if [ $? -eq 0 ]
    then
      echo "[`date`,INFO] ${file} ended successfully" >> ${log_file}
    else
      echo "[`date`,INFO] ${file} ended unsuccessfully" >> ${log_file}
    fi
    #3.后处理
done
echo "[`date`,INFO] hive_clear_manager.sh of peroid ${date} Ended!" >> ${log_file}
