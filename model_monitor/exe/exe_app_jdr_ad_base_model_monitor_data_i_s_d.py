#!/usr/bin/env python3
# ===============================================================================
#
#         FILE: exe_app_jdr_ad_base_model_monitor_data_i_s_d.py
#
#        USAGE: ./exe_app_jdr_ad_base_model_monitor_data_i_s_d.py
#
#  DESCRIPTION: 基础模型数据监控脚本
#
#      OPTIONS: ---
# REQUIREMENTS: ---
#         BUGS: ---
#        NOTES:
#       AUTHOR: puchangchun1@jd.com
#      COMPANY: jd.com
#      VERSION: 1.0
#      CREATED: 2023-07-27
#    ORC_TABLE: ad.ad_base_model_click | app.app_ad_business_model_flow_center_external_wide
#    TGT_TABLE: app.app_jdr_ad_base_model_monitor_data_i_s_d
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
ftime = datetime.datetime.strptime(ftime_std, '%Y%m%d').strftime('%Y%m%d')  # 遗留处理

tab_name = 'app.app_jdr_ad_base_model_monitor_data_i_s_d'

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


def get_app_base_model_click_placementid_monitor_data_sql(dt):
    # 1-1-1 广告位为空的明细层监控（站外）
    base_placement_id_null_sql = """
    SELECT
        ad_traffic_type,  --- 渠道类型
        """ + combine_play_type + """ AS combine_play_type,    --- '播放模式', 
    
        count(1) as all_clicks,   --- 总点击量
        sum(if(placement_id in (''), 1, 0)) as placementid_isnull_clicks,    --- 广告位为空的数量
    
        NULL AS consumption,
        NULL AS placementid_isnull_consumption,
        
        NULL AS repage_finance_clicks,           
        NULL AS repage_last_placementid_isnull_finance_clicks,
        
        NULL AS finance_clicks,
        NULL AS clickid_isnull_finance_clicks,
    
        NULL AS summary_finance_clicks,
        NULL AS detail_finance_clicks,
        
        NULL AS detail_rtb_join_c2s_clicks,
        NULL AS detail_rtb_clicks,
        
        NULL AS detail_s2s_clicks,
        NULL AS detail_c2s_clicks,
        
        NULL AS summary_cpc_consumption,
        NULL AS detail_cpc_consumption
    FROM ad.ad_base_model_click
    WHERE 
        dt='""" + dt + """' AND
        loc = '2'
    GROUP BY
        ad_traffic_type, 
        combine_play_type
    """

    # 1-1-2 广告位为空报宽表消耗监控
    app_placementid_isnull_consumption_sql = """
    SELECT
        ad_traffic_type,  --- 渠道类型
        """ + combine_play_type + """ AS combine_play_type,    --- 播放模式
        
        NULL AS all_clicks, 
        NULL AS placementid_isnull_clicks,  
    
        sum(consumption) as consumption,   --- 总消耗
        sum(if(placement_id in (''), consumption, 0)) as placementid_isnull_consumption,    --- 广告位为空的消耗
        
        NULL AS repage_finance_clicks,           
        NULL AS repage_last_placementid_isnull_finance_clicks,
        
        NULL AS finance_clicks,
        NULL AS clickid_isnull_finance_clicks,
    
        NULL AS summary_finance_clicks,
        NULL AS detail_finance_clicks,
        
        NULL AS detail_rtb_join_c2s_clicks,
        NULL AS detail_rtb_clicks,
        
        NULL AS detail_s2s_clicks,
        NULL AS detail_c2s_clicks,
        
        NULL AS summary_cpc_consumption,
        NULL AS detail_cpc_consumption
    FROM app.app_ad_business_model_flow_center_external_wide
    WHERE 
        dt='""" + dt + """' AND
    GROUP BY
        ad_traffic_type, 
        combine_play_type
    """

    # 1-2、中间页二跳对应上一跳广告位id是否为空监控
    # 拆分ATT，监控点击基础表（RTB分区）中上一跳广告位id为空比例
    # 过滤条件：站内外=中间页
    # 监控表：ad_base2_click → ad_base_model_click
    # 监控指标：二跳点击上一跳广告位为空比例（比例=上一跳广告位为空点击量/总计费点击量）
    base_1to2_placement_id_null_sql = """
    SELECT
        ad_traffic_type,  --- 渠道类型
        """ + combine_play_type + """ AS combine_play_type,    --- 播放模式
        
        NULL AS all_clicks,   
        NULL AS placementid_isnull_clicks, 
    
        NULL AS consumption,
        NULL AS placementid_isnull_consumption,
        
        count(1) AS repage_finance_clicks, --- 中间页的计费点击量
        sum(if(last_pos_id in ('','0'), 1, 0)) AS repage_last_placementid_isnull_finance_clicks,  --- 上一跳广告位为空的计费点击量
        
        NULL AS finance_clicks,
        NULL AS clickid_isnull_finance_clicks,
    
        NULL AS summary_finance_clicks,
        NULL AS detail_finance_clicks,
        
        NULL AS detail_rtb_join_c2s_clicks,
        NULL AS detail_rtb_clicks,
        
        NULL AS detail_s2s_clicks,
        NULL AS detail_c2s_clicks,
        
        NULL AS summary_cpc_consumption,
        NULL AS detail_cpc_consumption
     
    FROM ad.ad_base_model_click
    WHERE 
        dt = '""" + dt + """' AND 
        ( dp IN ('RTB_all') AND is_bill != 1 ) AND --- 计费点击(排除gdt)
	    loc = '3' --- 卡中间页
    GROUP BY
        ad_traffic_type, 
        combine_play_type
    """

    # 1-3 点击日志中click_id为空监控 (站外)
    # 拆分ATT*播放模式，统计计费分区中click_id为空比例
    # 监控表：ad_base_model_click
    # 监控指标：统计计费分区中click_id为空数量、统计计费分区中click_id为空比例（比例=click_id为空点击量/总计费点击量）
    base_click_id_null_sql = """
    SELECT
        ad_traffic_type,  --- 渠道类型
        """ + combine_play_type + """ AS combine_play_type,    --- 播放模式 
        NULL AS all_clicks,
        NULL AS placementid_isnull_clicks,    
    
        NULL AS consumption,
        NULL AS placementid_isnull_consumption,
        
        NULL AS repage_finance_clicks,           
        NULL AS repage_last_placementid_isnull_finance_clicks,
        
        count(1) AS finance_clicks,  --- 站外总计费点击数量 
        sum(if(click_id in ( '' ), 1, 0)) AS clickid_isnull_finance_clicks, --- 点击id为空的计费点击数量
    
        NULL AS summary_finance_clicks,
        NULL AS detail_finance_clicks,
        
        NULL AS detail_rtb_join_c2s_clicks,
        NULL AS detail_rtb_clicks,
        
        NULL AS detail_s2s_clicks,
        NULL AS detail_c2s_clicks,
        
        NULL AS summary_cpc_consumption,
        NULL AS detail_cpc_consumption        

    FROM ad.ad_base_model_click
    WHERE 
        dt = '""" + dt + """' AND 
        ( dp IN ('RTB_all') AND is_bill != 1 )  AND --- 计费点击(排除gdt)
        loc = '2' --- 站外
    GROUP BY
        ad_traffic_type, 
        combine_play_type
    """

    # 2-1 计费点击统计数 与 计费点击明细数 diff监控
    # 计费点击统计数与明细数diff监控
    # 拆ATT*播放模式，统计运营报表统计点击量与回传点击明细diff
    # 过滤条件：站内外=站外 & 合约效果=效果 & 播放模式=媒体播放
    # 监控指标：点击量diff（点击量diff=计费点击统计数-计费点击明细数）
    finance_clicks_diff_sql = """
    SELECT 
        ad_traffic_type,
        combine_play_type,
        NULL AS all_clicks,   --- 总点击量
        NULL AS placementid_isnull_clicks,    --- 广告位为空的数量
    
        NULL AS consumption,
        NULL AS placementid_isnull_consumption,
        
        NULL AS repage_finance_clicks,           
        NULL AS repage_last_placementid_isnull_finance_clicks,
        
        NULL AS finance_clicks,
        NULL AS clickid_isnull_finance_clicks,
    
        summary_finance_clicks, 
        detail_finance_clicks,
        
        NULL AS detail_rtb_join_c2s_clicks,
        NULL AS detail_rtb_clicks,
        
        NULL AS detail_s2s_clicks,
        NULL AS detail_c2s_clicks,
        
        NULL AS summary_cpc_consumption,
        NULL AS detail_cpc_consumption        

    FROM 
        (
            SELECT
                ad_traffic_type, 
                """ + combine_play_type + """ AS combine_play_type,  
                SUM(clicks) AS summary_finance_clicks,
                NULL AS detail_finance_clicks
            FROM app.app_ad_business_model_flow_center_external_wide
            WHERE 
                dt='""" + dt + """' AND 
                sales_type = '效果' AND --- 合约效果
                resource_type = 'external' --- 站外
            GROUP BY
                ad_traffic_type, 
                combine_play_type
            
            UNION ALL
                
            SELECT
                ad_traffic_type,  
                """ + combine_play_type + """ AS combine_play_type,   
                NULL AS summary_finance_clicks,
                count(1) AS detail_finance_clicks
            FROM ad.ad_base_model_click
            WHERE 
                dt='""" + dt + """' AND 
                dp IN ('RTB_all','GDT') AND is_bill != 1 AND --- 统计计费分区反作弊后的点击量（PRD给出的条件）
                loc = '2'
            GROUP BY
                ad_traffic_type, 
                combine_play_type
        )
    WHERE 
        combine_play_type = '媒体播放'
    """

    # 2-2 计费分区点击明细 与 c2s点击 关联比例 稳定性监控
    # 监控指标：c2s点击关联比例（c2s点击关联比例=rtb分区点击关联上c2s的点击量/RTB分区总点击量）
    # 代码实现：
    c2s_rtb_click_ratio_sql = """
    SELECT
        ad_traffic_type,
        combine_play_type,
        NULL AS all_clicks,  
        NULL AS placementid_isnull_clicks,  
    
        NULL AS consumption,
        NULL AS placementid_isnull_consumption,
        
        NULL AS repage_finance_clicks,           
        NULL AS repage_last_placementid_isnull_finance_clicks,
        
        NULL AS finance_clicks,
        NULL AS clickid_isnull_finance_clicks,
    
        NULL AS summary_finance_clicks,
        NULL AS detail_finance_clicks,
        
        detail_rtb_join_c2s_clicks, --- 关联c2s点击数（忠元）
        detail_rtb_clicks, --- rtb分区总有效点击量
        
        NULL AS detail_s2s_clicks,
        NULL AS detail_c2s_clicks,
        
        NULL AS summary_cpc_consumption,
        NULL AS detail_cpc_consumption        
    FROM
        (
            SELECT
                ad_traffic_type,  --- 渠道类型
                """ + combine_play_type + """ AS combine_play_type,  
                count(1) AS detail_rtb_join_c2s_clicks, --- 点击表关联c2s点击数
                NULL AS detail_rtb_clicks --- 点击表rtb分区点击数
            FROM 
                --- 计算c2s关联量（忠元）
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
                        dt = '""" + dt + """'
                        AND event_type IN ('0', '1', '3', '4') --- 'c2s'
                        AND loc = '2'  --- 站外广告
                        AND delivery_system_type IN ('0', '12') --- JZT广告投放系统
                        AND business_type IN ('4', '256', '2048') --- 
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
                    WHERE 
                        dt = '""" + dt + """'
                        AND dp in ('RTB_all')
                        AND loc = '2'
                        AND delivery_system_type IN ('0', '12')
                        AND business_type IN ('4', '256', '2048')
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
                rtb.rtb_sid is NOT NULL 
            GROUP BY 
                ad_traffic_type, 
                combine_play_type
        
            UNION ALL
            
            --- 计算rtb_all总点击量（限定站外、有效点击） 疑问：需要跟上面rtb分区的卡数条件一样吗？
            SELECT
                ad_traffic_type,  --- 渠道类型
                """ + combine_play_type + """ AS combine_play_type,
                NULL AS detail_rtb_join_c2s_clicks,
                count(1) AS detail_rtb_clicks 
            FROM
                ad.ad_base_model_click
            WHERE 
                dt = '""" + dt + """'
                AND dp = 'RTB_all'
                AND loc = '2'
                AND is_bill != '1' 
            GROUP BY
                ad_traffic_type,
                combine_play_type
        ) 
    """

    # 2-3 s2s与c2s点击的diff监控
    # 分att*播放模式，监控rtb+nc分区 s2s与c2s的点击diff
    # 监控表：ad_base_model_click
    # 监控指标：s2s点击量、c2s点击量、两个点击diff
    detail_event_type_clicks_diff_sql = """
    SELECT
        ad_traffic_type,  --- 渠道类型
        """ + combine_play_type + """ AS combine_play_type,    --- 播放模式 
        NULL AS all_clicks, 
        NULL AS placementid_isnull_clicks, 
    
        NULL AS consumption,
        NULL AS placementid_isnull_consumption,
        
        NULL AS repage_finance_clicks,           
        NULL AS repage_last_placementid_isnull_finance_clicks,
        
        NULL AS finance_clicks,
        NULL AS clickid_isnull_finance_clicks,
    
        NULL AS summary_finance_clicks,
        NULL AS detail_finance_clicks,
        
        NULL AS detail_rtb_join_c2s_clicks,
        NULL AS detail_rtb_clicks,
        
        sum(if(event_type NOT IN (0, 1, 3, 4) , 1, 0)) AS detail_s2s_clicks, --- s2s点击量 待更新
        sum(if(event_type IN (0, 1, 3, 4), 1, 0)) AS detail_c2s_clicks, --- c2s点击量 待更新
        
        NULL AS summary_cpc_consumption,
        NULL AS detail_cpc_consumption        

    FROM ad.ad_base_model_click
    WHERE 
        dt='""" + dt + """' AND
        dp in ( 'RTB_all','rtbnc') AND
        loc = '2' --- 卡站外
    GROUP BY
        ad_traffic_type, 
        combine_play_type    
    """

    # 2-4  计费点击明细统计消耗与网关回传单元粒度消耗 diff监控
    # 按点击计费的流量，拆ATT*播放模式，观测点击明细消耗与统计消耗diff
    # 过滤条件：计费类型 （ ad_billing_type ）=cpc，且业务类型=商家竞价广告
    wide_detail_consumption_diff_sql = """
    SELECT 
        ad_traffic_type,
        combine_play_type,
        
        NULL AS all_clicks,   --- 总点击量
        NULL AS placementid_isnull_clicks,    --- 广告位为空的数量
    
        NULL AS consumption,
        NULL AS placementid_isnull_consumption,
        
        NULL AS repage_finance_clicks,           
        NULL AS repage_last_placementid_isnull_finance_clicks,
        
        NULL AS finance_clicks,
        NULL AS clickid_isnull_finance_clicks,
    
        NULL AS summary_finance_clicks,
        NULL AS detail_finance_clicks,
        
        NULL AS detail_rtb_join_c2s_clicks,
        NULL AS detail_rtb_clicks,
        
        NULL AS detail_s2s_clicks,
        NULL AS detail_c2s_clicks,
        
        summary_cpc_consumption, 
        detail_cpc_consumption
    FROM 
        (
            SELECT
                ad_traffic_type,  --- 渠道类型
                """ + combine_play_type + """ AS combine_play_type,   --- '播放模式', 
                SUM(consumption) AS summary_cpc_consumption,
                NULL AS detail_cpc_consumption
            FROM app.app_ad_business_model_flow_center_external_wide
            WHERE 
                dt='""" + dt + """' AND 
                LOWER(ad_comsumption_type) = 'cpc' AND --- 计费类型
                ad_business_type IN ('2048', '33554432', '4', '256', '67108864', '524288') AND --- 限定“商家竞价广告”
                sales_type = '效果' AND --- 限定“商家竞价广告” 
                resource_type = 'external' --- 限定“商家竞价广告” 
            GROUP BY
                ad_traffic_type, 
                combine_play_type
                
            UNION ALL
    
            SELECT
                ad_traffic_type,  --- 渠道类型
                """ + combine_play_type + """ AS combine_play_type,    --- '播放模式', 
                NULL AS summary_cpc_consumption,
                SUM(total_price) AS detail_cpc_consumption
            FROM ad.ad_base_model_click
            WHERE 
                dt='""" + dt + """' AND 
                LOWER(ad_billing_type) = 'cpc' AND --- 计费类型
                ((dp = 'RTB_all' AND is_bill != 1) OR dp = 'GDT') AND --- 来源PRD的卡数条件
                loc = '2' --- 站外
            GROUP BY
                ad_traffic_type, 
                combine_play_type
        ) 
    """

    return """
    INSERT OVERWRITE TABLE """ + tab_name + """ PARTITION(dt = '""" + dt + """') 
    SELECT
        ad_traffic_type,
        combine_play_type,
        
        MAX(NVL(all_clicks,0)) as all_clicks, 
        MAX(NVL(placementid_isnull_clicks,0)) as placementid_isnull_clicks, 
    
        MAX(NVL(consumption,0)) as consumption,
        MAX(NVL(placementid_isnull_consumption,0)) as placementid_isnull_consumption,
    
        MAX(NVL(repage_finance_clicks,0)) as repage_finance_clicks,           
        MAX(NVL(repage_last_placementid_isnull_finance_clicks,0)) as repage_last_placementid_isnull_finance_clicks,
        
        MAX(NVL(finance_clicks,0)) as finance_clicks,
        MAX(NVL(clickid_isnull_finance_clicks,0)) as clickid_isnull_finance_clicks,
    
        MAX(NVL(summary_finance_clicks,0)) as summary_finance_clicks,
        MAX(NVL(detail_finance_clicks,0)) as detail_finance_clicks,
        
        MAX(NVL(detail_rtb_join_c2s_clicks,0)) as detail_rtb_join_c2s_clicks,
        MAX(NVL(detail_rtb_clicks,0)) as detail_rtb_clicks,
        
        MAX(NVL(detail_s2s_clicks,0)) as detail_s2s_clicks, 
        MAX(NVL(detail_c2s_clicks,0)) as detail_c2s_clicks,
        
        MAX(NVL(summary_cpc_consumption,0)) as summary_cpc_consumption, 
        MAX(NVL(detail_cpc_consumption,0)) as detail_cpc_consumption
    
    FROM 
        (
        """ + base_placement_id_null_sql + """  
        UNION ALL 
        """ + app_placementid_isnull_consumption_sql + """ 
        UNION ALL 
        """ + base_1to2_placement_id_null_sql + """  
        UNION ALL 
        """ + base_click_id_null_sql + """ 
        UNION ALL 
        """ + finance_clicks_diff_sql + """ 
        UNION ALL 
        """ + c2s_rtb_click_ratio_sql + """ 
        UNION ALL 
        """ + detail_event_type_clicks_diff_sql + """ 
        UNION ALL 
        """ + wide_detail_consumption_diff_sql + """ 
        )
    GROUP BY
        ad_traffic_type,
        combine_play_type
    """


while (int(ftime_std) <= int(ftime_end)):
    ht = HiveTask()
    dt = datetime.datetime.strptime(ftime_std, '%Y%m%d').strftime('%Y-%m-%d')
    exe_sql_app_base_model_click_placementid_monitor_data = h_udf + h_env + get_app_base_model_click_placementid_monitor_data_sql(
        dt)
    print("print sql : " + exe_sql_app_base_model_click_placementid_monitor_data)
    ht.exec_sql(
        schema_name='app',
        table_name=tab_name,
        sql=exe_sql_app_base_model_click_placementid_monitor_data,
        exec_engine='spark',
        spark_resource_level='high',
        retry_with_hive=False,
        spark_args=['--conf spark.sql.hive.mergeFiles=true']
    )
    ftime_std = (datetime.datetime.strptime(ftime_std, '%Y%m%d') + datetime.timedelta(days=1)).strftime('%Y%m%d')
