#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ===============================================================================
# FILE:exe_hive2ck_app_tc_visitpath_merge_result_report_dt_day_d.py
# USAGE:流量中心访问路径聚合表-平台：app+周期：day
# DESCRIPTION:
# REQUIREMENTS:
# BUGS:
# NOTES:
# AUTHOR:adsz
# COMPANY:
# VERSION:1.0
# CREATED:
# REVIEWER:
# REVISION:注：uv和tc_user_visits指标无稳态和快照之分，故底表均有值；但为了方便忠元聚合，
#          写入ck时，将稳态的uv和tc_user_visits全部置零；但是历史只需稳态，无需置零
# ===============================================================================
# ====================================推数包下载====================================
jar = "hive-2-ck_v1.3.0.jar"
import os

cmd = """wget http://ads-sz.s3-internal.cn-north-1.jdcloud-oss.com/ads_sz/%s -O %s""" % (jar, jar)
print(cmd + '...')
status = os.system(cmd)
if status != 0:
    print('hive-2-ck_v1.3.0.jar download Failed!')
else:
    print('hive-2-ck_v1.3.0.jar download Success!')


# ====================================集群信息配置====================================
# 集群配置信息
clusterName = "LFRH_CK_Pub_119"
# 用于查询集群信息的域名，并且用来作为CK中zookeeper中表路径使用，可选任意节点域名或者ip
clientHosts = "ckpub119.olap.jd.com"
clusterUser = "etl_ck_share_jdad_sz"
clusterPass = "maT86JWq2AxF-a_exGTO"
# 多实列部署（域名端口，ips上tcp端口与http端口映射）默认8123
port = "2000,9600-8623,9700-8723,9800-8823"
# ====================================数据表配置====================================
# 源数据库hive数据库
hive_database = "app"
# 源数据表hive数据表
hive_table = "tc_visitpath_merge_result_report_dt"
# 目标数据库clickhouse中的数据库
clickhouse_database = "report"
# 目标数据库clickhouse中的数据表，分布式表会在表名后加_d
clickhouse_tableName = "app_tc_visitpath_merge_result_report_day_local"
import sys

# 接受传入参数时间，开始时间，结束时间
startDate = sys.argv[1]
endDate = sys.argv[2]
pattern = sys.argv[3]       # sys.argv[3] pattern: 'normal'-日常跑数; 'history'-历史数据回刷-只需稳态数据
# 查询数据sql语句
normal_sql = """
    SELECT
        dt,
        platform,
        dp,
        lvl,
        is_visit_multi_chan,
        tc_visit_chan,
        user_visit_freq,
        user_life_cycle_type,
        visit_chan_cate2_cds_with_times,
        CASE WHEN dp='stable' THEN 0 ELSE uv END AS uv,
        CASE WHEN dp='stable' THEN 0 ELSE tc_user_visits END AS tc_user_visits,
        tc_user_visits_before_deal_t1,
        deal_user_num_t1,
        deal_order_num_toc_t1,
        deal_after_prefr_amount_1_toc_t1,
        tc_user_visits_before_deal_t2,
        deal_user_num_t2,
        deal_order_num_toc_t2,
        deal_after_prefr_amount_1_toc_t2,
        cast(ceiling(rand() * 30000) as int) as hash_key_for_shard,
        path_user_visit_freq,
        user_visitcycle,
        user_buycycle 
    FROM app.app_tc_visitpath_merge_result_report_dt
    WHERE 
        dt >= '%s' and dt <= '%s'
        AND tp = 'day'
""" % (startDate, endDate)

history_sql = """
    SELECT
        dt,
        platform,
        dp,
        lvl,
        is_visit_multi_chan,
        tc_visit_chan,
        user_visit_freq,
        user_life_cycle_type,
        visit_chan_cate2_cds_with_times,
        uv,
        tc_user_visits,
        tc_user_visits_before_deal_t1,
        deal_user_num_t1,
        deal_order_num_toc_t1,
        deal_after_prefr_amount_1_toc_t1,
        tc_user_visits_before_deal_t2,
        deal_user_num_t2,
        deal_order_num_toc_t2,
        deal_after_prefr_amount_1_toc_t2,
        cast(ceiling(rand() * 30000) as int) as hash_key_for_shard,
        path_user_visit_freq,
        user_visitcycle,
        user_buycycle 
    FROM app.app_tc_visitpath_merge_result_report_dt
    WHERE 
        dt >= '%s' and dt <= '%s'
        AND tp = 'day'
""" % (startDate, endDate)

if pattern == 'normal':
    hivesql = normal_sql
elif pattern == 'history':
    hivesql = history_sql
else:
    raise EOFError('请指定pattern=normal或者pattern=history.')
# 适用于分区使用函数的分区更新如cityHash64(partner_code)%64、toYYYYMM(toDate(dt))
# 更新分区值请使用函数转换后的值 多个值用,间隔，实际分区可不是日期类型,也可使用下面的update_partitions更新数据
# startDate = 1,2,3,4
# endDate = 1,2,3,4

# shard分布id clickhouse的shard分发,单字段。
# 注意:多字段可以增加coalesce(cast(abs(hash(A,B,C)) as int) %集群分片数,0) hash_key_for_shard
distribute_id = "hash_key_for_shard"
# 分区
partitionColumns = "toYYYYMMDD(toDate(dt))"
# 索引
primaryKey = "platform, lvl, tc_visit_chan, visit_chan_cate2_cds_with_times, user_life_cycle_type, user_visit_freq"

# -----以上是需要用户配置的信息，下面为高级配置可不修改------
# 更新分区 如多级ck分区('2021-01-01','owner',9),('2021-01-02','owner',13)或者更改分区值的(1),(2),(3),(4)
# 注意:非多级分区此字段不要填 update_partitions =""
update_partitions = ""
# ck表引擎 如ReplicatedReplacingMergeTree:更新字段（update_time）
ck_engine = "ReplicatedMergeTree"
# ck表settings 如index_granularity = 128,
ck_settings = "storage_policy = 'jdob_ha',index_granularity = 8192"
# ck表删除分区方式1:on cluster,0:大数据量各分片上执行
dropFlag = 0
# 数据插入批次大小 默认10万
batchSize = 500000
# 控制读取Hive并发数,repartition数*分片数，建议[1,5]
readerNum = 15
# 控制写入Clickhouse并发数,总写入进程数/分片数,建议[25,100]
writerNum = 25
# 推数前是否删除就数据1删除0不删除
deleteFlag = 1
# 分片副本数默认为2:自动读取系统表可不设置
shardGroupNum = 2
# 判断是否是多实例补数的集群，默认为1
clusterModeCurrency = 1
# system,clusters集群信息表名称
clusterInfo = "clusters"
# ck表存储策略
storagePolicy = "jdob_ha"
#是否字典表，数据*分片数
replicaDict = 0
#是否版本化
versionFlag = 0
#间隔时间ms
waitTime = 1000
#字段是否允许存null
nullableFlag = 0


import re
import subprocess


def trimFun(strStr):
    return '"' + re.sub(r'\n*\s*(>|<)\n*\s*', r'\1', strStr) + '"'


# 清除缓存方法，部分推数完成后需要清除jimdb缓存的可以调用此方法
def updateCache():
    cmd = """curl http://g.jsf.jd.local/com.jd.goldeneye.dataserviceschema.client.DimensionSchemaService/test/updateCache/1049981/jsf/50000?service=brand_plusAnalysis """
    print(cmd)
    v = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    result = v.stdout.readlines()
    return result


hivesql = trimFun(hivesql)
distribute_id = trimFun(distribute_id)
partitionColumns = trimFun(partitionColumns)
primaryKey = trimFun(primaryKey)
update_partitions = trimFun(update_partitions)
ck_engine = trimFun(ck_engine)
ck_settings = trimFun(ck_settings)

print(hivesql)

sDate = startDate.replace('-','')
eDate = endDate.replace('-','')

cmd = """spark-submit --class com.jd.clickhouse.Spark2ClickHouseNew --master yarn --deploy-mode client --conf spark.speculation=false --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.sql.viewPermission.enabled=true --conf spark.sql.parser.quotedRegexColumnNames=false --executor-cores 15 --executor-memory 12g --driver-memory 10g --num-executors 100 --total-executor-cores 1000 %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s""" % (
    jar, hive_database, hive_table, clickhouse_database, clickhouse_tableName, distribute_id, sDate, eDate,
    clusterName,
    clientHosts, clusterUser, clusterPass, readerNum, deleteFlag, shardGroupNum, hivesql, primaryKey,
    partitionColumns,
    clusterModeCurrency, clusterInfo, port, storagePolicy, ck_engine, ck_settings, dropFlag, batchSize,
    update_partitions, writerNum,replicaDict,versionFlag,waitTime,nullableFlag)
print(cmd)
from HiveTask import HiveTask

task = HiveTask()
result = task.run_shell_cmd(cmd)
print(str(result))
print(str(result[0]))
if (str(result[0]) == '0'):
    print('hive2ck Insert Data Success')
    exit(0);
else:
    print('hive2ck Insert Data Failed')
    exit(1)