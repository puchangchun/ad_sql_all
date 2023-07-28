#!/usr/bin/env python3
# ===============================================================================
#
#         FILE: exe_app_base_model_pv_log_monitor_data.py
#
#        USAGE: ./exe_app_base_model_pv_log_monitor_data.py
#
#  DESCRIPTION: 渠道归因相关监控

#      OPTIONS: ---
# REQUIREMENTS: ---
#         BUGS: ---
#        NOTES:
#       AUTHOR: puchangchun1@jd.com
#      COMPANY: jd.com
#      VERSION: 1.0
#      CREATED: 2023-07-24
#    TGT_TABLE: app.app_base_model_pv_log_monitor_data
# ===============================================================================
import sys
import time
import datetime
import os
from HiveTask import HiveTask

sys.path.append(os.getenv('HIVE_TASK'))
homePath = os.getenv('HOME')

# 输入参数：python3 ***.py start_date end_date
if (len(sys.argv) > 4 or len(sys.argv) <= 1):
    sys.exit("ParameterNumberException.")
std = sys.argv[1]
end = sys.argv[2]
dp = sys.argv[3]
print("std: %s, end:%s, end:%s" % (std, end, dp))
# 参数校验，如果参数格式解析异常则抛出异常
try:
    time.strptime(std, "%Y-%m-%d")
    time.strptime(end, "%Y-%m-%d")
except Exception as e:
    sys.exit("ParameterParseException.")
ftime_std = datetime.datetime.strptime(std, '%Y-%m-%d').strftime('%Y%m%d')
ftime_end = datetime.datetime.strptime(end, '%Y-%m-%d').strftime('%Y%m%d')

h_udf = """
    --- import udf
"""
h_env = """
    --- set env
    set hive.optimize.correlation = true;
    set hive.exec.dynamic.partition.mode = nonstrict;
    set hive.exec.dynamic.partition = true;
    SET hive.exec.max.dynamic.partitions = 100000;
    SET hive.exec.max.dynamic.partitions.pernode = 100000;
    set hive.exec.parallel = true;
    set hive.exec.parallel.thread.number = 8;
    set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
    set hive.hadoop.supports.splittable.combineinputformat = true;
    set mapreduce.input.fileinputformat.split.maxsize = 256000000;
    set mapreduce.input.fileinputformat.split.minsize.per.node = 256000000;
    set mapreduce.input.fileinputformat.split.minsize.per.rack = 256000000;
    set hive.merge.mapfiles = true;
    set hive.merge.mapredfiles = true;
    set hive.merge.size.per.task = 256000000;
    set hive.merge.smallfiles.avgsize = 256000000;
    set spark.sql.hive.mergeFiles = true;
    set spark.sql.parser.quotedRegexColumnNames = false;
    set mapreduce.map.memory.mb=32768;
    set mapreduce.map.java.opts='-Xmx24576M';
    set mapreduce.reduce.memory.mb=32768;
    set mapreduce.reduce.java.opts='-Xmx24576M';
"""



def get_app_base_model_pv_log_monitor_data_sql(dt):
    # 2.2.2 四级渠道统计浏览UV与计费点击关联统计浏览PV diff监控
    # 监控表：ad_base_model_pv_log_with_click
    # 监控指标：浏览关联比例（浏览关联比例=计费点击关联浏览量/四级渠道统计浏览量）
    pv_join_click_sql = """
    SELECT
        pv_chan_first_cate_cd,
        pv_chan_second_cate_cd,        
        pv_chan_third_cate_cd,        
        pv_chan_fourth_cate_cd,
        count(1) as pv, 
        sum(if(is_join_clikc_flag in ('1'), 1, 0)) as join_click_pv
    FROM
        ad.ad_base_model_pv_log_with_click
    WHERE 
        dt = '""" + dt + """' 
    GROUP BY
        pv_chan_first_cate_cd,
        pv_chan_second_cate_cd,        
        pv_chan_third_cate_cd,        
        pv_chan_fourth_cate_cd     
    """

    # 2.2.3 浏览表中jdv_utm_term中是否有宏替换失败的情况
    # 分四级渠道，监控jdv_utm_term宏替换失败比例
    # 监控表：ad_base_model_pv_log_with_click
    # 监控指标：监控jdv_utm_term宏替换失败的浏览量、jdv_utm_term宏替换失败比例（比例=jdv_utm_term宏替换失败的浏览量/总浏览）
    dv_utm_term_replace_failed_sql = """
    SELECT
        pv_chan_first_cate_cd,
        pv_chan_second_cate_cd,        
        pv_chan_third_cate_cd,        
        pv_chan_fourth_cate_cd,
        sum(if(is_join_clikc_flag in ('1'), 1, 0)) as jdv_utm_term_replace_faild
    FROM
        ad.ad_base_model_pv_log_with_click
    WHERE 
        dt = '""" + dt + """' 
    GROUP BY
        pv_chan_first_cate_cd,
        pv_chan_second_cate_cd,        
        pv_chan_third_cate_cd,        
        pv_chan_fourth_cate_cd     
    """

    return """
    INSERT OVERWRITE TABLE app.app_base_model_pv_log_monitor_data PARTITION(dt = '""" + dt + """') 
    SELECT
    pv_chan_first_cate_cd,
    pv_chan_second_cate_cd,
    pv_chan_third_cate_cd,       
    pv_chan_fourth_cate_cd,
    
    max(pv),
    max(join_click_pv), 
    
    max(jdv_utm_term_replace_faild)
    
    FROM 
        """ + pv_join_click_sql + """
        UNION ALL 
        """ + dv_utm_term_replace_failed_sql + """
    GROUP BY
        pv_chan_first_cate_cd,
        pv_chan_second_cate_cd,        
        pv_chan_third_cate_cd,        
        pv_chan_fourth_cate_cd  
    """


while (int(ftime_std) <= int(ftime_end)):
    ht = HiveTask()
    tab_name = 'app.app_base_model_pv_log_monitor_data'
    dt = datetime.datetime.strptime(ftime_std, '%Y%m%d').strftime('%Y-%m-%d')
    exe_sql_app_base_model_pv_log_monitor_data = h_udf + h_env + get_app_base_model_pv_log_monitor_data_sql(dt)
    print("print sql : " + exe_sql_app_base_model_pv_log_monitor_data)
    ht.exec_sql(
        schema_name='app',
        table_name=tab_name,
        sql=exe_sql_app_base_model_pv_log_monitor_data,
        exec_engine='spark',
        spark_resource_level='high',
        retry_with_hive=False,
        spark_args=['--conf spark.sql.hive.mergeFiles=true']
    )
    ftime_std = (datetime.datetime.strptime(ftime_std, '%Y%m%d') + datetime.timedelta(days=1)).strftime('%Y%m%d')

