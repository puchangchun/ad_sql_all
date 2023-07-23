#!/usr/bin/env python3
# ===============================================================================
#
#         FILE: app_jdr_external_wide_core_dashboard_v4_a_s_d.py
#
#        USAGE: ./app_jdr_external_wide_core_dashboard_v4_a_s_d.py
#
#  DESCRIPTION: 经营看板_离线成本
#
#      OPTIONS: ---
# REQUIREMENTS: ---
#         BUGS: ---
#        NOTES:
#       AUTHOR: chenlei160@jd.com
#      COMPANY: jd.com
#      VERSION: 1.0
#      CREATED: 2023-05-15
#    TGT_TABLE: app_jdr_external_core_budget_meida_type_data
# ===============================================================================
import sys
import time
import datetime
import calendar
import os
from HiveTask import HiveTask
from multiprocessing import Pool

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
ftime = datetime.datetime.strptime(ftime_std, '%Y%m%d').strftime('%Y%m%d') # 遗留处理
dt = (datetime.datetime.strptime(std, '%Y-%m-%d') + datetime.timedelta(days=-0))

h_udf = """
    --- import udf
    ADD JAR hdfs://ns22019/user/mart_jd_ad/mart_jd_ad_sz/zhaoyinze/jd_ad_data_hive_udf-1.0-SNAPSHOT.jar;
    CREATE TEMPORARY FUNCTION IsDirtyDevice AS 'com.jd.ad.data.udf.IsDirtyDevice';
    CREATE TEMPORARY FUNCTION MapToStr AS 'com.jd.ad.data.udf.MapToStr';
    CREATE TEMPORARY FUNCTION ArrayToStr AS 'com.jd.ad.data.udf.ArrayToStr';
    CREATE TEMPORARY FUNCTION GetConversionPathNew AS 'com.jd.ad.data.udf.GetConversionPathNew';
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

# 播放模式维度计算逻辑
combine_play_type = """
        CASE
          WHEN ad_traffic_group in ('2', '4', '12', '23', '21', '1', '-999999')
          AND ad_traffic_type not in ('315', '854', '157', '317') THEN '京东播放'
          WHEN ad_traffic_type in ('854', '315', '157', '317')
          OR (
            ad_traffic_type in ('99', '237')
            AND campaign_type in ('8', '13', '77')
          ) THEN '共播'
          ELSE '媒体播放'
        END
    """

def get_app_base_model_click_placemenid_monitor_data_sql(dt):
    # ad_base_model_click 一行数据表示一次广告位的点击

    # 广告位为空明细层监控
    base_placement_id_null_sql =  """
    SELECT
        ad_traffic_type,  --- 渠道类型
        """ + combine_play_type + """ AS combine_play_type,    --- '播放模式', 
    
        count(1) as all_clicks,   --- 总点击量
        sum(if(placement_id in (''), 1, 0)) as placementid_isnull_clicks    --- 广告位为空的数量
        
    FROM ad.ad_base_model_click
    WHERE 
        dt='""" + dt + """'
    GROUP BY
        ad_traffic_type, 
        combine_play_type
    
    """

    # 广告位为空报宽表消耗监控
    app_placemenid_isnull_consumption_sql = """
    SELECT
        ad_traffic_type,  --- 渠道类型
        """ + combine_play_type + """ AS combine_play_type,    --- '播放模式', 
    
        sum(consumption) as consumption,   --- 总消耗
        sum(if(placement_id in (''), 1, 0)) as placemenid_isnull_consumption    --- 广告位为空的数量
        
    FROM app.app_ad_business_model_flow_center_external_wide
    WHERE 
        dt='""" + dt + """'
    GROUP BY
        ad_traffic_type, 
        combine_play_type
    """

    # 中间页二跳对应上一跳广告位id是否为空监控
    base_1to2_placement_id_null_sql = """
        SELECT
        ad_traffic_type,  --- 渠道类型
        """ + combine_play_type + """ AS combine_play_type,    --- '播放模式', 
        count(1) AS base2_finance_clicks,            --- double COMMENT '总计费点击量',
        sum(if(last_pos_id in ('','0'), 1, 0)) AS clickid_isnull_clicks       
    FROM ad.ad_base_model_click
    WHERE 
        dt='""" + dt + """' AND 
        dp = 'RTB_all' AND
	    is_bill != '1' AND --- 是否作弊
	    loc = '3' --- 站内外标识 枚举内容？？
    GROUP BY
        ad_traffic_type, 
        combine_play_type
    """

    # 点击日志中click_id为空监控，总计费点击是否跟上面的合并
    base_click_id_null_sql = """
    SELECT
        ad_traffic_type,  --- 渠道类型
        """ + combine_play_type + """ AS combine_play_type,    --- '播放模式', 
        count(1) AS finance_clicks,   
        sum(if(click_id in ( '' ), 1, 0)) AS clickid_isnull_clicks
    FROM ad.ad_base_model_click
    WHERE 
        dt='""" + dt + """' AND 
        dp = 'RTB_all' AND
	    is_bill != '1'  --- 计费点击
    GROUP BY
        ad_traffic_type, 
        combine_play_type
    """

    # 计费点击统计数 与 计费点击明细数 diff监控
    finance_clicks_diff_sql = """
    SELECT 
        detail.ad_traffic_type as ad_traffic_type,
        detail.combine_play_type as combine_play_type,
        wide_finance_clicks, --- 存在 att*播放模式 （相互）关联不上的情况，宽表点击就为null
        detail_finance_clicks
    FROM 
        (
            (
                SELECT
                    ad_traffic_type,  --- 渠道类型
                    """ + combine_play_type + """ AS combine_play_type,   --- '播放模式', 
                    sum(clicks) AS wide_finance_clicks
                FROM app.app_ad_business_model_flow_center_external_wide
                WHERE 
                    dt='""" + dt + """' AND 
                    ad_business_type in ('2048', '33554432', '4', '256', '67108864', '524288') AND --- 限定“商家竞价广告”
                    sales_type = '效果' --- 限定“商家竞价广告”
                GROUP BY
                    ad_traffic_type, 
                    combine_play_type
            ) wide
            
            RIGHT JOIN
                
            (
                SELECT
                    ad_traffic_type,  --- 渠道类型
                    """ + combine_play_type + """ AS combine_play_type,    --- '播放模式', 
                    count(1) AS detail_finance_clicks
                FROM ad.ad_base_model_click
                WHERE 
                    dt='""" + dt + """' AND 
                    ( ( dp = 'RTB_all' AND is_bill != '1' ) or dp = 'gdt' ) --- 统计计费分区的点击明细
                GROUP BY
                    ad_traffic_type, 
                    combine_play_type
            ) detail
            ON (wide.ad_traffic_type = detail.ad_traffic_type and wide.combine_play_type = detail.combine_play_type)
        )
    """

    # 计费分区点击明细 与 c2s点击 关联比例 稳定性监控
    # 监控指标：c2s点击关联比例（c2s点击关联比例=rtb分区点击关联上c2s的点击量/RTB分区总点击量）
    c2s_rtb_click_ratio_sql = """
    SELECT
        rtb_clicks.ad_traffic_type as ad_traffic_type,
        rtb_clicks.combine_play_type as combine_play_type,
        c2s_clicks.detail_rtb_joinc2s_clicks as detail_rtb_joinc2s_clicks, --- 没关联上就会为null
        rtb_clicks.detail_rtb_clicks as detail_rtb_clicks
    FROM
        (
            SELECT
                ad_traffic_type,  --- 渠道类型
                """ + combine_play_type + """ AS combine_play_type,  
                count(1) as detail_rtb_joinc2s_clicks --- 点击表关联c2s点击数
            FROM 
                (
                    SELECT
                        ad_traffic_group, --- combine_play_type
                        ad_traffic_type, --- combine_play_type
                        campaign_type, --- combine_play_type
                        CASE
                            WHEN ad_traffic_type IN ('99', '122', '251')
                            THEN click_id
                            ELSE sid
                        END AS c2s_sid,
                        CASE
                            WHEN ad_type = '3'
                            THEN item_sku_id
                            ELSE material_id
                        END AS join_key
                    FROM
                        ad.ad_base_model_click
                    WHERE
                        dt = '"""+dt+"""'
                        AND event_type IN ('0', '1', '3', '4')
                        AND loc = '2'
                        AND delivery_system_type IN ('0', '12')
                        AND business_type IN ('4', '256', '2048', '67108864')
                ) c2s
            LEFT JOIN
                (
                    SELECT
                        CASE
                            WHEN ad_traffic_type IN('99', '122', '251')
                            THEN click_id
                            ELSE sid
                        END AS rtb_sid,
                        CASE
                            WHEN ad_type = '3'
                            THEN item_sku_id
                            ELSE material_id
                        END AS rtb_join_key
                    FROM
                        ad.ad_base_model_click
                    WHERE --- 点击分区的判断逻辑？
                        dt = '"""+dt+"""'
                        AND dp = 'RTB_all'
                        AND loc = '2'
                        AND delivery_system_type IN ('0', '12')
                        AND business_type IN ('2048', '256', '4', '67108864')
                        AND is_bill != '1'
                    GROUP BY
                        rtb_sid,
                        rtb_join_key
                ) rtb
            ON
                c2s.c2s_sid = rtb.rtb_sid
                AND c2s.join_key = rtb.rtb_join_key
                AND COALESCE(rtb.rtb_sid, '0') != '0'
                AND COALESCE(rtb.rtb_join_key, '0') != '0'
            WHERE
                rtb.rtb_sid is NOT NULL --- 关联上的
            GROUP BY 
                ad_traffic_type, 
                combine_play_type
        ) c2s_clicks    
    right join
        (
            SELECT
                ad_traffic_type,  --- 渠道类型
                """ + combine_play_type + """ AS combine_play_type,
                count(1) as detail_rtb_clicks --- 点击表rtb分区点击数
            FROM
                ad.ad_base_model_click
            WHERE --- 点击rtb分区的判断逻辑？
                dt = '"""+dt+"""'
                AND dp = 'RTB_all'
                AND loc = '2'
                AND delivery_system_type IN ('0', '12')
                AND business_type IN ('2048', '256', '4', '67108864')
                AND is_bill != '1' 
            GROUP BY
                ad_traffic_type,
                combine_play_type
        ) rtb_clicks
    ON
        rtb_clicks.ad_traffic_type = c2s_clicks.ad_traffic_type AND
        rtb_clicks.combine_play_type = c2s_clicks.combine_play_type
    """

    # s2s与c2s点击的diff监控
    # 分att*播放模式，监控rtb+nc分区 s2s与c2s的点击diff
    # 监控表：ad_base_model_click
    # 监控指标：s2s点击量、c2s点击量、两个点击diff
    # event_type	string	用来区分client2server和Server2Server 1： C2S 2：S2S
    # CASE
    #         WHEN event_type NOT IN(0, 1, 3, 4)
    #         THEN 's2s'
    #         ELSE 'c2s'
    #     END as event_type
    detail_event_type_clicks_diff = """
    SELECT
        ad_traffic_type,  --- 渠道类型
        """ + combine_play_type + """ AS combine_play_type,    --- '播放模式', 
        sum(if(event_type NOT IN (0, 1, 3, 4), 1, 0)) AS detail_s2s_clicks,
        sum(if(event_type IN (0, 1, 3, 4), 1, 0)) AS detail_c2s_clicks
    FROM ad.ad_base_model_click
    WHERE 
        dt='""" + dt + """' AND 
        dp in ('RTB_all', 'rtbnc') --- 统计计费分区的点击明细
    GROUP BY
        ad_traffic_type, 
        combine_play_type    
    """

    # 计费点击明细统计消耗与网关回传单元粒度消耗 diff监控
    # 按点击计费的流量，拆ATT*播放模式，观测点击明细消耗与统计消耗diff
    wide_detail_consumption_diff_sql = """
    SELECT 
        detail.ad_traffic_type as ad_traffic_type,
        detail.combine_play_type as combine_play_type,
        wide_cpc_consumption, --- 存在 att*播放模式 （相互）关联不上的情况，宽表点击就为null
        detail_cpc_consumption
    FROM 
        (
            (
                SELECT
                    ad_traffic_type,  --- 渠道类型
                    """ + combine_play_type + """ AS combine_play_type,   --- '播放模式', 
                    sum(consumption) AS wide_cpc_consumption
                FROM app.app_ad_business_model_flow_center_external_wide
                WHERE 
                    dt='""" + dt + """' AND 
                    ad_business_type in ('2048', '33554432', '4', '256', '67108864', '524288') AND --- 限定“商家竞价广告”
                    sales_type = '效果' AND --- 限定“商家竞价广告” 
                    ad_billing_type = 'cpc'
                GROUP BY
                    ad_traffic_type, 
                    combine_play_type
            ) wide
            
            RIGHT JOIN
                
            (
                SELECT
                    ad_traffic_type,  --- 渠道类型
                    """ + combine_play_type + """ AS combine_play_type,    --- '播放模式', 
                    sum(total_price) AS detail_cpc_consumption
                FROM ad.ad_base_model_click
                WHERE 
                    dt='""" + dt + """' AND 
                    ( ( dp = 'RTB_all' AND is_bill != '1' ) or dp = 'gdt' ) --- 统计计费分区的点击明细
                GROUP BY
                    ad_traffic_type, 
                    combine_play_type
            ) detail
            ON (wide.ad_traffic_type = detail.ad_traffic_type and wide.combine_play_type = detail.combine_play_type)
        )
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
        ad.ad_base_model_click
    WHERE --- 点击rtb分区的判断逻辑？
        dt = '"""+dt+"""'
        AND dp = 'RTB_all'
    GROUP BY
        pv_chan_first_cate_cd,
        pv_chan_second_cate_cd,        
        pv_chan_third_cate_cd,        
        pv_chan_fourth_cate_cd     
    """

    # 2.2.3 浏览表中jdv_utm_term中是否有宏替换失败的情况

while (int(ftime) <= int(ftime_end)):

    ht = HiveTask()
    tab_name = 'app.app_base_model_click_placemenid_monitor_data'

    h_sql = ""

    ht.exec_sql(schema_name='app', table_name=tab_name, sql=h_sql, exec_engine='spark', spark_resource_level='high',
                retry_with_hive=False, spark_args=['--conf spark.sql.hive.mergeFiles=true'])
# --conf spark.sql.parquet.mergeSchema=true