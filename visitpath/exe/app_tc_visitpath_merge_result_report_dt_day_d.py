#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ===============================================================================
#
#         FILE: app_tc_visitpath_merge_result_report_dt_day_d.py
#
#        USAGE: ./app_tc_visitpath_merge_result_report_dt_day_d.py
#
#  DESCRIPTION: 流量中心访问路径预聚合hive表-平台：app+周期：day
#
#      OPTIONS: ---
# REQUIREMENTS: ---
#         BUGS: ---
#        NOTES:
#       AUTHOR: liying364@jd.com puchangchun1@jd.com
#      COMPANY: jd.com
#      VERSION: 1.1
#      CREATED: 2023-07-21
#    ORC_TABLE:
#    TGT_TABLE:
# ===============================================================================
import sys
import time
import datetime
import os
from HiveTask import HiveTask

sys.path.append(os.getenv('HIVE_TASK'))
homePath = os.getenv("HOME")
# 输入参数：python3 ***.py start_date end_date
if (len(sys.argv) > 4 or len(sys.argv) <= 1):
    sys.exit("ParameterNumberException.")
std = sys.argv[1]
end = sys.argv[2]
dp = sys.argv[3]  # sys.argv[3] dp: 'snapshot'-成交快照; 'stable'-成交稳态
print("std: %s, end:%s, end:%s" % (std, end, dp))
# 参数校验，如果参数格式解析异常则抛出异常
try:
    time.strptime(std, "%Y-%m-%d")
    time.strptime(end, "%Y-%m-%d")
except Exception as e:
    sys.exit("ParameterParseException.")
ftime_std = datetime.datetime.strptime(std, '%Y-%m-%d').strftime('%Y%m%d')
ftime_end = datetime.datetime.strptime(end, '%Y-%m-%d').strftime('%Y%m%d')
ftime = datetime.datetime.strptime(ftime_std, '%Y%m%d').strftime('%Y%m%d')

while (int(ftime) <= int(ftime_end)):
    ht = HiveTask()
    yesterday = datetime.datetime.strptime(ftime, '%Y%m%d').strftime('%Y-%m-%d')

    stable_sql = """
    INSERT OVERWRITE TABLE app.app_tc_visitpath_merge_result_report_dt PARTITION(dt='""" + yesterday + """',tp,dp) 
    SELECT
        'APP' AS platform,
        CASE 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00111111'  THEN '0' 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00011111'  THEN '1' 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00101111'  THEN '2' 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00001111'  THEN '3' 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00110111'  THEN '4' 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00110011'  THEN '5' 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00000000'  THEN '7' ---- 最细维度
        END AS lvl, --- 纬度组合 
        is_visit_multi_chan, --- 触达渠道类型
        tc_visit_chan, --- 触达渠道
        user_visit_freq, --- 访问频次 （每天的pv_id（jdv or session）去重得到）
        user_life_cycle_type, --- 用户生命周期类型
        NULL AS visit_chan_cate2_cds_with_times, --- 访问路径【渠道｜次数】
        COUNT(DISTINCT browser_uniq_id) AS uv, --- uv
        SUM(tc_visit_times) AS tc_user_visits, --- 访问次数
        0 AS tc_user_visits_before_deal_t1, --- 成交前访问次数
        0 AS deal_user_num_toc_t1, --- 成交用户数量
        0 AS deal_order_num_toc_t1, --- 成交父订单数量
        0 AS deal_after_prefr_amount_1_toc_t1, --- 成交GMV
        SUM(tc_user_visits_before_deal) AS tc_user_visits_before_deal_t2, --- 成交前访问次数【稳态】
        COUNT(DISTINCT CASE WHEN attr = 'order' THEN user_id ELSE NULL END) AS deal_user_num_toc_t2, --- 成交用户数量【稳态】
        COUNT(DISTINCT CASE WHEN parent_sale_ord_id = '' THEN NULL ELSE parent_sale_ord_id END) AS deal_order_num_toc_t2, --- 成交父订单数量【稳态】
        SUM(tc_deal_after_prefr_amount_1) AS deal_after_prefr_amount_1_toc_t2, --- 成交GMV【稳态】
        NULL AS path_user_visit_freq, --- 路径访问频次
        --- 后添加字段
        user_visitcycle, --- 用户访问周期类型
        user_buycycle, --- 用户购买周期类型
        'day' AS tp,
        'stable' AS dp
    FROM
    (
        --- 得到用户属性、订单金额与访问路径，用于统计 路径 与 用户订单 的关系
        SELECT
            COALESCE(unif_user_log_acct, browser_uniq_id) AS user_id,
            NULL AS browser_uniq_id,
            parent_sale_ord_id,
            MIN(attr) AS attr,
            MAX(user_visitcycle) AS user_visitcycle, --- 用户访问周期类型
            MAX(user_buycycle) AS user_buycycle, --- 用户购买周期类型
            MAX(user_life_cycle_type) AS user_life_cycle_type,
            MAX(tc_visit_chan) AS tc_visit_chan,
            --- ==== 修改 ====
            CASE 
                WHEN MAX(tc_visit_chan_num) > 3 THEN '>=4' 
                WHEN MAX(tc_visit_chan_num) = 3 THEN '3' 
                WHEN MAX(tc_visit_chan_num) = 2 THEN '2' 
                WHEN MAX(tc_visit_chan_num) = 1 THEN '1' 
                ELSE ''
            END AS is_visit_multi_chan,  --- 渠道访问类型
            
            MAX(user_visit_freq) AS user_visit_freq,
            0 AS tc_visit_times,
            MAX(tc_user_visits_before_deal) AS tc_user_visits_before_deal,
            MAX(tc_deal_after_prefr_amount_1) AS tc_deal_after_prefr_amount_1
        FROM app.app_tc_visitpath_user_detail_report_dt
        WHERE dt = '""" + yesterday + """'
            AND platform = 'APP'
            AND tp = 'day'
            AND dp = 'stable'
        GROUP BY 
            COALESCE(unif_user_log_acct, browser_uniq_id),
            parent_sale_ord_id
        
        UNION ALL
        
        --- 去重uuid，附带用户属性，（统计 浏览 与 访问路径的关系，uv/pv）
        SELECT
            NULL AS user_id,
            browser_uniq_id,
            NULL AS parent_sale_ord_id,
            '' AS attr,
            MAX(user_visitcycle) AS user_visitcycle, --- 用户访问周期类型
            MAX(user_buycycle) AS user_buycycle, --- 用户购买周期类型
            MAX(user_life_cycle_type) AS user_life_cycle_type,
            MAX(tc_visit_chan) AS tc_visit_chan, --- 访问渠道
            --- ==== 修改 ====
            --- 改变渠道访问类型的标记
            CASE 
                WHEN MAX(tc_visit_chan_num) > 3 THEN '>=4' 
                WHEN MAX(tc_visit_chan_num) = 3 THEN '3' 
                WHEN MAX(tc_visit_chan_num) = 2 THEN '2' 
                WHEN MAX(tc_visit_chan_num) = 1 THEN '1' 
                ELSE ''
            END AS is_visit_multi_chan,  --- 渠道访问类型
            MAX(user_visit_freq) AS user_visit_freq, --- 访问频次
            0 AS tc_visit_times,
            0 AS tc_user_visits_before_deal,
            0 AS tc_deal_after_prefr_amount_1
        FROM app.app_tc_visitpath_user_detail_report_dt
        WHERE dt = '""" + yesterday + """'
            AND platform = 'APP'
            AND tp = 'day'
            AND dp = 'stable'
            AND browser_uniq_id IS NOT NULL AND browser_uniq_id != ''
        GROUP BY 
            browser_uniq_id
            
        UNION ALL
        
        --- 得到 访问次数 与 路径 的关系，
        SELECT
            COALESCE(unif_user_log_acct, browser_uniq_id) AS user_id,
            NULL AS browser_uniq_id,
            NULL AS parent_sale_ord_id,
            '' AS attr,
            MAX(user_visitcycle) AS user_visitcycle, --- 用户访问周期类型
            MAX(user_buycycle) AS user_buycycle, --- 用户购买周期类型
            MAX(user_life_cycle_type) AS user_life_cycle_type,
            MAX(tc_visit_chan) AS tc_visit_chan,
            --- ==== 修改 ====
            CASE 
                WHEN MAX(tc_visit_chan_num) > 3 THEN '>=4' 
                WHEN MAX(tc_visit_chan_num) = 3 THEN '3' 
                WHEN MAX(tc_visit_chan_num) = 2 THEN '2' 
                WHEN MAX(tc_visit_chan_num) = 1 THEN '1' 
                ELSE ''
            END AS is_visit_multi_chan,  --- 渠道访问类型
            MAX(user_visit_freq) AS user_visit_freq,
            MAX(tc_visit_times) AS tc_visit_times,
            0 AS tc_user_visits_before_deal,
            0 AS tc_deal_after_prefr_amount_1
        FROM app.app_tc_visitpath_user_detail_report_dt
        WHERE dt = '""" + yesterday + """'
            AND platform = 'APP'
            AND tp = 'day'
            AND dp = 'stable'
        GROUP BY 
            COALESCE(unif_user_log_acct, browser_uniq_id)
    ) aa
    GROUP BY 
        is_visit_multi_chan, --- 是否多渠道
        tc_visit_chan, --- 触达渠道
        user_visit_freq, --- 访问频次
        user_life_cycle_type, --- 用户生命周期类型
        user_visitcycle, --- 用户访问周期类型
        user_buycycle --- 用户购买周期类型
    grouping sets
    (
        (), --0总-00111111
        (is_visit_multi_chan),  --1是否多渠道-00011111
        (tc_visit_chan), --2触达渠道-00101111
        (is_visit_multi_chan, tc_visit_chan), --3是否多渠道+触达渠道-00001111
        (user_visit_freq),  --4访问频次-00110111
        (user_visit_freq, user_life_cycle_type), --5用户生命周期+访问频次-00110011
        (user_life_cycle_type, user_buycycle, user_visitcycle, is_visit_multi_chan, user_visit_freq, tc_visit_chan) ---7 00000000 用户生命周期+购买周期+访问周期+是否多渠道+访问频次+触达渠道
    )
    
    UNION ALL
    
    --- 访问渠道频次 区别 用户访问频次
    
    SELECT
        'APP' AS platform,
        '6'  AS lvl,
        is_visit_multi_chan, --- 添加 是否多渠道
        NULL AS tc_visit_chan,
        NULL AS user_visit_freq,
        user_life_cycle_type,
        visit_chan_cate2_cds_with_times, --- 访问路径
        COUNT(DISTINCT browser_uniq_id) AS uv,
        SUM(tc_visit_times) AS tc_user_visits,
        0 AS tc_user_visits_before_deal_t1,
        0 AS deal_user_num_toc_t1,
        0 AS deal_order_num_toc_t1,
        0 AS deal_after_prefr_amount_1_toc_t1,
        SUM(tc_user_visits_before_deal) AS tc_user_visits_before_deal_t2,
        COUNT(DISTINCT CASE WHEN attr = 'order' THEN user_id ELSE NULL END) AS deal_user_num_toc_t2,
        COUNT(DISTINCT CASE WHEN parent_sale_ord_id = '' THEN NULL ELSE parent_sale_ord_id END) AS deal_order_num_toc_t2,
        SUM(tc_deal_after_prefr_amount_1) AS deal_after_prefr_amount_1_toc_t2,
        path_user_visit_freq, --- 访问频次
        --- 后添加字段
        user_visitcycle, --- 用户访问周期类型
        user_buycycle, --- 用户购买周期类型
        'day' AS tp,
        'stable' AS dp
    FROM
    (
        --- 和上面一样
        --- 去重uuid，附带用户属性，（统计 浏览 与 访问路径的关系，uv/pv）
        SELECT
            COALESCE(unif_user_log_acct, browser_uniq_id) AS user_id,
            NULL AS browser_uniq_id,
            parent_sale_ord_id,
            MIN(attr) AS attr,
            MAX(user_visitcycle) AS user_visitcycle, --- 用户访问周期类型
            MAX(user_buycycle) AS user_buycycle, --- 用户购买周期类型
            CASE 
                WHEN MAX(tc_visit_chan_num) > 3 THEN '>=4' 
                WHEN MAX(tc_visit_chan_num) = 3 THEN '3' 
                WHEN MAX(tc_visit_chan_num) = 2 THEN '2' 
                WHEN MAX(tc_visit_chan_num) = 1 THEN '1' 
                ELSE ''
            END AS is_visit_multi_chan,  --- 渠道访问类型
            MAX(user_life_cycle_type) AS user_life_cycle_type,
            visit_chan_cate2_cds_with_times,
            path_user_visit_freq,
            0 AS tc_visit_times,
            MAX(tc_user_visits_before_deal) AS tc_user_visits_before_deal,
            MAX(tc_deal_after_prefr_amount_1) AS tc_deal_after_prefr_amount_1
        FROM app.app_tc_visitpath_user_detail_report_dt
        WHERE dt = '""" + yesterday + """'
            AND platform = 'APP'
            AND tp = 'day'
            AND dp = 'stable'
        GROUP BY 
            COALESCE(unif_user_log_acct, browser_uniq_id),
            parent_sale_ord_id,
            path_user_visit_freq,
            visit_chan_cate2_cds_with_times
        
        UNION ALL
        
        SELECT
            NULL AS user_id,
            browser_uniq_id,
            NULL AS parent_sale_ord_id,
            '' AS attr,
            MAX(user_visitcycle) AS user_visitcycle, --- 用户访问周期类型
            MAX(user_buycycle) AS user_buycycle, --- 用户购买周期类型
            CASE 
                WHEN MAX(tc_visit_chan_num) > 3 THEN '>=4' 
                WHEN MAX(tc_visit_chan_num) = 3 THEN '3' 
                WHEN MAX(tc_visit_chan_num) = 2 THEN '2' 
                WHEN MAX(tc_visit_chan_num) = 1 THEN '1' 
                ELSE ''
            END AS is_visit_multi_chan,  --- 渠道访问类型
            MAX(user_life_cycle_type) AS user_life_cycle_type,
            visit_chan_cate2_cds_with_times,
            path_user_visit_freq,
            0 AS tc_visit_times,
            0 AS tc_user_visits_before_deal,
            0 AS tc_deal_after_prefr_amount_1
        FROM app.app_tc_visitpath_user_detail_report_dt
        WHERE dt = '""" + yesterday + """'
            AND platform = 'APP'
            AND tp = 'day'
            AND dp = 'stable'
            AND browser_uniq_id IS NOT NULL AND browser_uniq_id != ''
        GROUP BY 
            browser_uniq_id,
            path_user_visit_freq,
            visit_chan_cate2_cds_with_times
            
        UNION ALL
        
        SELECT
            COALESCE(unif_user_log_acct, browser_uniq_id) AS user_id,
            NULL AS browser_uniq_id,
            NULL AS parent_sale_ord_id,
            '' AS attr,
            MAX(user_visitcycle) AS user_visitcycle, --- 用户访问周期类型
            MAX(user_buycycle) AS user_buycycle, --- 用户购买周期类型
            CASE 
                WHEN MAX(tc_visit_chan_num) > 3 THEN '>=4' 
                WHEN MAX(tc_visit_chan_num) = 3 THEN '3' 
                WHEN MAX(tc_visit_chan_num) = 2 THEN '2' 
                WHEN MAX(tc_visit_chan_num) = 1 THEN '1' 
                ELSE ''
            END AS is_visit_multi_chan,  --- 是否多渠道
            MAX(user_life_cycle_type) AS user_life_cycle_type,
            visit_chan_cate2_cds_with_times,
            path_user_visit_freq,
            MAX(tc_visit_times) AS tc_visit_times,
            0 AS tc_user_visits_before_deal,
            0 AS tc_deal_after_prefr_amount_1
        FROM app.app_tc_visitpath_user_detail_report_dt
        WHERE dt = '""" + yesterday + """'
            AND platform = 'APP'
            AND tp = 'day'
            AND dp = 'stable'
        GROUP BY 
            COALESCE(unif_user_log_acct, browser_uniq_id),
            path_user_visit_freq, --- 
            visit_chan_cate2_cds_with_times --- 渠道
    ) aa
    GROUP BY 
        user_life_cycle_type, --- 用户生命周期类型
        user_buycycle, --- 用户购买周期类型
        user_visitcycle, --- 用户访问周期类型
        is_visit_multi_chan, --- 是否多渠道道
        path_user_visit_freq, --- 路径访问频次
        visit_chan_cate2_cds_with_times --- 访问路径
        ; --6 (生命周期+购买周期+访问周期+是否多渠道+路径访问频次+访问路径)
    """

    snapshot_sql = """
    INSERT OVERWRITE TABLE app.app_tc_visitpath_merge_result_report_dt PARTITION(dt='""" + yesterday + """',tp,dp) 
    SELECT
        'APP' AS platform,
        CASE 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00111111'  THEN '0' 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00011111'  THEN '1' 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00101111'  THEN '2' 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00001111'  THEN '3' 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00110111'  THEN '4' 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00110011'  THEN '5' 
            WHEN lpad(bin(CAST(GROUPING__ID AS bigint)), 8, '0') = '00000000'  THEN '7' ---- 最细维度
        END AS lvl, --- 纬度组合 
        is_visit_multi_chan,
        tc_visit_chan,
        user_visit_freq,
        user_life_cycle_type,
        NULL AS visit_chan_cate2_cds_with_times,
        COUNT(DISTINCT browser_uniq_id) AS uv,
        SUM(tc_visit_times) AS tc_user_visits,
        SUM(tc_user_visits_before_deal) AS tc_user_visits_before_deal_t1,
        COUNT(DISTINCT CASE WHEN attr = 'order' THEN user_id ELSE NULL END) AS deal_user_num_toc_t1,
        COUNT(DISTINCT CASE WHEN parent_sale_ord_id = '' THEN NULL ELSE parent_sale_ord_id END) AS deal_order_num_toc_t1,
        SUM(tc_deal_after_prefr_amount_1) AS deal_after_prefr_amount_1_toc_t1,
        0 AS tc_user_visits_before_deal_t2,
        0 AS deal_user_num_toc_t2,
        0 AS deal_order_num_toc_t2,
        0 AS deal_after_prefr_amount_1_toc_t2,
        NULL AS path_user_visit_freq,
        --- 后添加字段
        user_visitcycle, --- 用户访问周期类型
        user_buycycle, --- 用户购买周期类型
        'day' AS tp,
        'snapshot' AS dp
    FROM
    (
        SELECT
            COALESCE(unif_user_log_acct, browser_uniq_id) AS user_id,
            NULL AS browser_uniq_id,
            parent_sale_ord_id,
            MIN(attr) AS attr,
            MAX(user_visitcycle) AS user_visitcycle, --- 用户访问周期类型
            MAX(user_buycycle) AS user_buycycle, --- 用户购买周期类型
            MAX(user_life_cycle_type) AS user_life_cycle_type,
            MAX(tc_visit_chan) AS tc_visit_chan,
            --- ==== 修改 ====
            --- 改变渠道访问类型的标记
            CASE 
                WHEN MAX(tc_visit_chan_num) > 3 THEN '>=4' 
                WHEN MAX(tc_visit_chan_num) = 3 THEN '3' 
                WHEN MAX(tc_visit_chan_num) = 2 THEN '2' 
                WHEN MAX(tc_visit_chan_num) = 1 THEN '1' 
                ELSE ''
            END AS is_visit_multi_chan,  --- 渠道访问类型
            MAX(user_visit_freq) AS user_visit_freq,
            0 AS tc_visit_times,
            MAX(tc_user_visits_before_deal) AS tc_user_visits_before_deal,
            MAX(tc_deal_after_prefr_amount_1) AS tc_deal_after_prefr_amount_1
        FROM app.app_tc_visitpath_user_detail_report_dt
        WHERE dt = '""" + yesterday + """'
            AND platform = 'APP'
            AND tp = 'day'
            AND dp = 'snapshot'
        GROUP BY 
            COALESCE(unif_user_log_acct, browser_uniq_id),
            parent_sale_ord_id
        
        UNION ALL
        
        SELECT
            NULL AS user_id,
            browser_uniq_id,
            NULL AS parent_sale_ord_id,
            '' AS attr,
            MAX(user_visitcycle) AS user_visitcycle, --- 用户访问周期类型
            MAX(user_buycycle) AS user_buycycle, --- 用户购买周期类型
            MAX(user_life_cycle_type) AS user_life_cycle_type,
            MAX(tc_visit_chan) AS tc_visit_chan,
            --- ==== 修改 ====
            CASE 
                WHEN MAX(tc_visit_chan_num) > 3 THEN '>=4' 
                WHEN MAX(tc_visit_chan_num) = 3 THEN '3' 
                WHEN MAX(tc_visit_chan_num) = 2 THEN '2' 
                WHEN MAX(tc_visit_chan_num) = 1 THEN '1' 
                ELSE ''
            END AS is_visit_multi_chan,  --- 渠道访问类型
            MAX(user_visit_freq) AS user_visit_freq,
            0 AS tc_visit_times,
            0 AS tc_user_visits_before_deal,
            0 AS tc_deal_after_prefr_amount_1
        FROM app.app_tc_visitpath_user_detail_report_dt
        WHERE dt = '""" + yesterday + """'
            AND platform = 'APP'
            AND tp = 'day'
            AND dp = 'snapshot'
            AND browser_uniq_id IS NOT NULL AND browser_uniq_id != ''
        GROUP BY 
            browser_uniq_id
            
        UNION ALL
        
        SELECT
            COALESCE(unif_user_log_acct, browser_uniq_id) AS user_id,
            NULL AS browser_uniq_id,
            NULL AS parent_sale_ord_id,
            '' AS attr,
            MAX(user_visitcycle) AS user_visitcycle, --- 用户访问周期类型
            MAX(user_buycycle) AS user_buycycle, --- 用户购买周期类型
            MAX(user_life_cycle_type) AS user_life_cycle_type,
            MAX(tc_visit_chan) AS tc_visit_chan,
            --- ==== 修改 ====
            CASE 
                WHEN MAX(tc_visit_chan_num) > 3 THEN '>=4' 
                WHEN MAX(tc_visit_chan_num) = 3 THEN '3' 
                WHEN MAX(tc_visit_chan_num) = 2 THEN '2' 
                WHEN MAX(tc_visit_chan_num) = 1 THEN '1' 
                ELSE ''
            END AS is_visit_multi_chan,  --- 渠道访问类型
            MAX(user_visit_freq) AS user_visit_freq,
            MAX(tc_visit_times) AS tc_visit_times,
            0 AS tc_user_visits_before_deal,
            0 AS tc_deal_after_prefr_amount_1
        FROM app.app_tc_visitpath_user_detail_report_dt
        WHERE dt = '""" + yesterday + """'
            AND platform = 'APP'
            AND tp = 'day'
            AND dp = 'snapshot'
        GROUP BY 
            COALESCE(unif_user_log_acct, browser_uniq_id)
    ) aa
    GROUP BY 
        is_visit_multi_chan, --- 是否多渠道
        tc_visit_chan, --- 触达渠道
        user_visit_freq, --- 访问频次
        user_life_cycle_type, --- 用户生命周期类型
        user_visitcycle, --- 用户访问周期类型
        user_buycycle --- 用户购买周期类型
    grouping sets
    (
        (), --0总-
        (is_visit_multi_chan),  --1是否多渠道-
        (tc_visit_chan), --2触达渠道-
        (is_visit_multi_chan, tc_visit_chan), --3是否多渠道+触达渠道-
        (user_visit_freq),  --4访问频次-
        (user_visit_freq, user_life_cycle_type), --5用户生命周期+访问频次-
        (user_life_cycle_type, user_buycycle, user_visitcycle, is_visit_multi_chan, user_visit_freq, tc_visit_chan) ---7 00000000 用户生命周期+购买周期+访问周期+是否多渠道+访问频次+触达渠道
    )
    
    UNION ALL
    
    SELECT
        'APP' AS platform,
        '6' AS lvl,
        is_visit_multi_chan, --- 7期新增
        NULL AS tc_visit_chan,
        NULL AS user_visit_freq,
        user_life_cycle_type,
        visit_chan_cate2_cds_with_times, --- 访问渠道路径
        COUNT(DISTINCT browser_uniq_id) AS uv,
        SUM(tc_visit_times) AS tc_user_visits,
        SUM(tc_user_visits_before_deal) AS tc_user_visits_before_deal_t1,
        COUNT(DISTINCT CASE WHEN attr = 'order' THEN user_id ELSE NULL END) AS deal_user_num_toc_t1,
        COUNT(DISTINCT CASE WHEN parent_sale_ord_id = '' THEN NULL ELSE parent_sale_ord_id END) AS deal_order_num_toc_t1,
        SUM(tc_deal_after_prefr_amount_1) AS deal_after_prefr_amount_1_toc_t1,
        0 AS tc_user_visits_before_deal_t2,
        0 AS deal_user_num_toc_t2,
        0 AS deal_order_num_toc_t2,
        0 AS deal_after_prefr_amount_1_toc_t2,
        path_user_visit_freq,
        user_visitcycle, --- 用户访问周期类型
        user_buycycle, --- 用户购买周期类型
        'day' AS tp,
        'snapshot' AS dp
    FROM
    (
        SELECT
            COALESCE(unif_user_log_acct, browser_uniq_id) AS user_id,
            NULL AS browser_uniq_id,
            parent_sale_ord_id,
            MIN(attr) AS attr,
            MAX(user_visitcycle) AS user_visitcycle, --- 用户访问周期类型
            MAX(user_buycycle) AS user_buycycle, --- 用户购买周期类型
            CASE 
                WHEN MAX(tc_visit_chan_num) > 3 THEN '>=4' 
                WHEN MAX(tc_visit_chan_num) = 3 THEN '3' 
                WHEN MAX(tc_visit_chan_num) = 2 THEN '2' 
                WHEN MAX(tc_visit_chan_num) = 1 THEN '1' 
                ELSE ''
            END AS is_visit_multi_chan,  --- 是否多渠道
            MAX(user_life_cycle_type) AS user_life_cycle_type,
            visit_chan_cate2_cds_with_times,
            path_user_visit_freq,
            0 AS tc_visit_times,
            MAX(tc_user_visits_before_deal) AS tc_user_visits_before_deal,
            MAX(tc_deal_after_prefr_amount_1) AS tc_deal_after_prefr_amount_1
        FROM app.app_tc_visitpath_user_detail_report_dt
        WHERE dt = '""" + yesterday + """'
            AND platform = 'APP'
            AND tp = 'day'
            AND dp = 'snapshot'
        GROUP BY 
            COALESCE(unif_user_log_acct, browser_uniq_id),
            parent_sale_ord_id,
            path_user_visit_freq,
            visit_chan_cate2_cds_with_times
        
        UNION ALL
        
        SELECT
            NULL AS user_id,
            browser_uniq_id,
            NULL AS parent_sale_ord_id,
            '' AS attr,
            MAX(user_visitcycle) AS user_visitcycle, --- 用户访问周期类型
            MAX(user_buycycle) AS user_buycycle, --- 用户购买周期类型
            CASE 
                WHEN MAX(tc_visit_chan_num) > 3 THEN '>=4' 
                WHEN MAX(tc_visit_chan_num) = 3 THEN '3' 
                WHEN MAX(tc_visit_chan_num) = 2 THEN '2' 
                WHEN MAX(tc_visit_chan_num) = 1 THEN '1' 
                ELSE ''
            END AS is_visit_multi_chan,  --- 是否多渠道
            MAX(user_life_cycle_type) AS user_life_cycle_type,
            visit_chan_cate2_cds_with_times,
            path_user_visit_freq,
            0 AS tc_visit_times,
            0 AS tc_user_visits_before_deal,
            0 AS tc_deal_after_prefr_amount_1
        FROM app.app_tc_visitpath_user_detail_report_dt
        WHERE dt = '""" + yesterday + """'
            AND platform = 'APP'
            AND tp = 'day'
            AND dp = 'snapshot'
            AND browser_uniq_id IS NOT NULL AND browser_uniq_id != ''
        GROUP BY 
            browser_uniq_id,
            path_user_visit_freq,
            visit_chan_cate2_cds_with_times
            
        UNION ALL
        
        SELECT
            COALESCE(unif_user_log_acct, browser_uniq_id) AS user_id,
            NULL AS browser_uniq_id,
            NULL AS parent_sale_ord_id,
            '' AS attr,
            MAX(user_visitcycle) AS user_visitcycle, --- 用户访问周期类型
            MAX(user_buycycle) AS user_buycycle, --- 用户购买周期类型
            CASE 
                WHEN MAX(tc_visit_chan_num) > 3 THEN '>=4' 
                WHEN MAX(tc_visit_chan_num) = 3 THEN '3' 
                WHEN MAX(tc_visit_chan_num) = 2 THEN '2' 
                WHEN MAX(tc_visit_chan_num) = 1 THEN '1' 
                ELSE ''
            END AS is_visit_multi_chan,  --- 是否多渠道
            MAX(user_life_cycle_type) AS user_life_cycle_type,
            visit_chan_cate2_cds_with_times,
            path_user_visit_freq,
            MAX(tc_visit_times) AS tc_visit_times,
            0 AS tc_user_visits_before_deal,
            0 AS tc_deal_after_prefr_amount_1
        FROM app.app_tc_visitpath_user_detail_report_dt
        WHERE dt = '""" + yesterday + """'
            AND platform = 'APP'
            AND tp = 'day'
            AND dp = 'snapshot'
        GROUP BY 
            COALESCE(unif_user_log_acct, browser_uniq_id),
            path_user_visit_freq,
            visit_chan_cate2_cds_with_times
    ) aa
    GROUP BY 
        user_life_cycle_type, --- 用户生命周期类型
        user_buycycle, --- 用户购买周期类型
        user_visitcycle, --- 用户访问周期类型
        is_visit_multi_chan, --- 是否多渠道道
        path_user_visit_freq, --- 路径访问频次
        visit_chan_cate2_cds_with_times --- 访问路径
        ; --6 (生命周期+购买周期+访问周期+是否多渠道+路径访问频次+访问路径)
    """

    if dp == 'stable':
        sql = stable_sql
    elif dp == 'snapshot':
        sql = snapshot_sql
    else:
        raise EOFError('请指定dp=stable或者dp=snapshot.')
    ht.exec_sql(schema_name='app', table_name='app_tc_visitpath_merge_result_report_dt', sql=sql,
                exec_engine='spark',
                spark_resource_level='high', spark_args=['--conf spark.sql.hive.mergeFiles=true'])
    # ftime + 1
    ftime = (datetime.datetime.strptime(ftime, '%Y%m%d') + datetime.timedelta(days=1)).strftime('%Y%m%d')
print("FixTask Has Finished.")
