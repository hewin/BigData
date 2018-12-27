
[tool name]:hive_clear
[function]:Daily clear hive relevant info ( contains drop hive table, drop partition of hive table, drop hdfs of hive table)
[field]:hive,bigdata

1.Step1 Add configuration file like tables_conf/ads.ads_tianwang_0level_token_tokenscore_ds.json
like following:
{
    "owner": "chenjianjun@ishumei.com",
    "if_drop_table": "false",
    "if_drop_partition": "true",
    "if_delete_hdfs": "true",
    "date_partition_name": "dt",
    "date_partition_format": "%Y%m%d",
    "hive_database": "ads",
    "hive_table": "ads_tianwang_0level_token_tokenscore_ds",
    "hive_table_location": "/user/data/hive/ads/ads_tianwang_0level_token_tokenscore_ds",
    "save_window": 15
}

2.Step2 Start the manager shell script
hive_clear_manager.sh [date]
or
set on crontab to set up a daily task.

3.What's more
If you have many hive tables to clear,you can use the generate_table_tool/run_sample.sh to create relevant configure file

4.If you you any problems or questions
Please freely contact with me on email(Gmail: wenxin.ext@gmail.com)
