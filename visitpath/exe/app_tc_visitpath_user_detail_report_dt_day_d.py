#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ===============================================================================
#
#         FILE: app_tc_visitpath_user_detail_report_dt_day_d.py
#
#        USAGE: ./app_tc_visitpath_user_detail_report_dt_day_d.py
#
#  DESCRIPTION: 流量中心访问路径用户粒度明细表-平台：app+周期：day
#
#      OPTIONS: ---
# REQUIREMENTS: ---
#         BUGS: ---
#        NOTES:
#       AUTHOR: puchangchun1@jd.com
#      COMPANY: jd.com
#      VERSION: 1.0
#      CREATED: 2023-07-10
#    ORC_TABLE:1.T+1的数据浏览、订单、uuid、归一pin用T+1,生命周期关联T+2, 登陆标签关联T+1
#          2.所有关联逻辑，pin和uuid均转小写
#    TGT_TABLE:注：下游使用该表时，需根据 COALESCE(unif_user_log_acct, browser_uniq_id)和
#    parent_sale_ord_id取MAX，再进行其他操作
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
dp = sys.argv[3]  # sys.argv[3] dp: 'snapshot'-成交快照; 'stable'-成交稳态; 'history'-2022年及以前成交稳态
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
dt = (datetime.datetime.strptime(std, '%Y-%m-%d') + datetime.timedelta(days=-0))

while (int(ftime) <= int(ftime_end)):
    ht = HiveTask()
    yesterday = datetime.datetime.strptime(ftime, '%Y%m%d').strftime('%Y-%m-%d')

    # 季度第一天
    date_quarter_first_dt = datetime.date(dt.year, (dt.month - 1) // 3 * 3 + 1, 1)
    quarter_first = date_quarter_first_dt.strftime("%Y-%m-%d")

    stable_sql = """
    ADD JAR hdfs://ns22019/user/mart_jd_ad/mart_jd_ad_sz/zhaoyinze/jd_ad_data_hive_udf-1.0-SNAPSHOT.jar;
    CREATE TEMPORARY FUNCTION IsDirtyDevice AS 'com.jd.ad.data.udf.IsDirtyDevice';
    CREATE TEMPORARY FUNCTION MapToStr AS 'com.jd.ad.data.udf.MapToStr';
    CREATE TEMPORARY FUNCTION ArrayToStr AS 'com.jd.ad.data.udf.ArrayToStr';
    CREATE TEMPORARY FUNCTION GetConversionPathNew AS 'com.jd.ad.data.udf.GetConversionPathNew';
    
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
    
    WITH
    --- 得到带用户信息的周期交易金额 
    orderdata_tab AS
    (
        SELECT
            unif_user_log_acct, --- 归一pin
            user_log_acct, --- 用户下单账号
            parent_sale_ord_id, --- 父订单号
            sale_ord_tm AS even_time, --- 下单时间
            SUM(after_prefr_amount_1) AS after_prefr_amount_1 --- 交易金额
        FROM 
            (
                SELECT
                    LOWER(TRIM(unif_user_log_acct)) AS unif_user_log_acct, --- 归一pin
                    LOWER(TRIM(user_log_acct)) AS user_log_acct, --- 用户下单账号
                    item_sku_id, --- 商品SKU编号
                    sale_ord_id, --- 子订单号
                    CASE WHEN type_c = 'C端' THEN parent_sale_ord_id ELSE '' END AS parent_sale_ord_id, --- 父订单号【只有C端订单有？】
                    SPLIT(sale_ord_tm, '\\\\\\\\.')[0] AS sale_ord_tm, --- 下单时间yyyy-MM-dd HH:MM:SS
                    MAX(CASE WHEN type_c = 'C端' THEN after_prefr_amount_1 ELSE 0.0 END) AS after_prefr_amount_1 --- 优惠后金额
                FROM 
                    app.app_mkt_d01_retail_ltm_ord_sku_di --- (零售LTM-基础数据模型)
                WHERE 
                    dt = '"""+yesterday+"""'
                    AND tp = 0 --- 刷新标记
                    AND dp = 'DTD' --- 时间周期
                    AND new_bs = 'APP' --- 平台
                    AND intraday_ord_deal_flag = '1' --- 订单成交标识
                GROUP BY 
                    LOWER(TRIM(unif_user_log_acct)),
                    LOWER(TRIM(user_log_acct)),
                    item_sku_id,
                    sale_ord_id,
                    CASE WHEN type_c = 'C端' THEN parent_sale_ord_id ELSE '' END,
                    SPLIT(sale_ord_tm, '\\\\\\\\.')[0] 
            ) innertab
        GROUP BY 
            unif_user_log_acct,
            user_log_acct,
            parent_sale_ord_id,
            sale_ord_tm
    ),
    --- 带用户信息的浏览记录表，下游主要使用该子查询来统计数据
    userinfo_tab AS
    (
        SELECT
            unif_user_log_acct,
            user_log_acct,
            browser_uniq_id,
            attr, --- 'visit or order'
            pv_id, --- jdv or session
            --- 为单渠道添加优先级
            CASE
                WHEN chan_second_cate_desc_bel = '直接流量' THEN CONCAT('1-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '搜索及信息流' THEN CONCAT('2-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '展示广告' THEN CONCAT('3-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '广告变现' THEN CONCAT('4-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '腾讯资源' THEN CONCAT('5-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '联盟' THEN CONCAT('6-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = 'APP投放引流' THEN CONCAT('7-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '付费其他' THEN CONCAT('8-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '免费其他' THEN CONCAT('9-',chan_second_cate_desc_bel) 
                ELSE NULL
            END AS chan_second_cate_desc_bel, --- 访问渠道
            MapToStr(MAP('event_time', even_time, 'event', attr, 'user_log_acct', COALESCE(unif_user_log_acct, browser_uniq_id), 'parent_sale_ord_id', parent_sale_ord_id, 'after_prefr_amount_1', after_prefr_amount_1, 'chan_second_cate_desc', NVL(chan_second_cate_desc_bel, ''), 'jdv', NVL(pv_id, ''))) AS info
        FROM
        (
            SELECT
                unif_user_log_acct,
                user_log_acct,
                browser_uniq_id,
                chan_second_cate_desc_bel,
                pv_id,
                '' AS parent_sale_ord_id,
                request_tm AS even_time,
                0 AS after_prefr_amount_1,
                'visit' AS attr
            FROM
                app.app_tc_sch_d14_traffic_plat_d --- 用户浏览表
            WHERE 
                --- dt = 'yesterday'
                dt = '"""+yesterday+"""'
                AND platform = 'APP'
                
            UNION ALL 
            
            SELECT
                unif_user_log_acct,
                user_log_acct, 
                NULL AS browser_uniq_id,
                NULL AS chan_second_cate_desc_bel, --- 增加了访问渠道的值域，影响到了
                NULL AS pv_id,
                parent_sale_ord_id,
                even_time,
                after_prefr_amount_1,
                'order' AS attr
            FROM 
                orderdata_tab --- 用户成交订单表
        ) aa
    )
    
    INSERT OVERWRITE TABLE app.app_tc_visitpath_user_detail_report_dt PARTITION(dt='"""+yesterday+"""',tp,dp)   --- dt=yesterday
    SELECT
        'APP' AS platform,   
        pintab.unif_user_log_acct AS unif_user_log_acct,
        user_log_acct,
        browser_uniq_id,
        attr,
        CASE  WHEN (pintab.unif_user_log_acct IS NULL OR init_acct_status = '0') THEN '无pin'
                WHEN pintab.unif_user_log_acct <> '' AND (user_life_cycle_type IS NULL OR user_life_cycle_type = '-1') THEN '生命周期未知(有pin)'  
        ELSE user_life_cycle_type END AS user_life_cycle_type,  
        parent_sale_ord_id,
        CASE 
            WHEN tc_visit_chan_num = 0 THEN ''
            WHEN tc_visit_chan_num <= 4 THEN tc_visit_chan_label
            ELSE '其他' 
        END AS tc_visit_chan, --- 触达渠道
        tc_visit_chan_num,
        visit_chan_cate2_cds_with_times,
        tc_user_app_visit_label AS user_visit_freq, --- 访问频次标签
        today_browse_times AS tc_visit_times, --- 访问次数（粒度：天）
        tc_user_visits_before_deal,
        after_prefr_amount_1 AS tc_deal_after_prefr_amount_1,
        CASE 
            WHEN ( visit_chan_cate2_all_times = 0 OR visit_chan_cate2_all_times IS NULL)
            THEN '0次'
            WHEN visit_chan_cate2_all_times < 7
            THEN CONCAT(CAST(visit_chan_cate2_all_times AS string), '次')
            WHEN visit_chan_cate2_all_times < 11
            THEN '[7-10]次'
            ELSE '>10次'
        END AS path_user_visit_freq,
        
        COALESCE(pin_user_visitcycle_app, uuid_user_visitcycle_app, '近365日未访') AS user_visitcycle, --- 访问周期
        COALESCE(pin_user_buycycle_all, uuid_user_buycycle_all, '历史未购') AS user_buycycle, --- 购买周期
        
        'day' AS tp,
        'stable' AS dp
    FROM
    (
        SELECT
            unif_user_log_acct,
            user_log_acct,
            browser_uniq_id
        FROM
            userinfo_tab
        GROUP BY
            browser_uniq_id,
            unif_user_log_acct,
            user_log_acct
    ) pintab --- pin粒度
    LEFT JOIN
    (
        SELECT
            user_id,
            attr,
            today_browse_times,
            CASE
                WHEN
                    (
                        today_browse_times = 0
                        OR today_browse_times IS NULL
                    )
                THEN '0次'
                WHEN today_browse_times < 7
                THEN CONCAT(CAST(today_browse_times AS string), '次')
                WHEN today_browse_times < 11
                THEN '[7-10]次'
                ELSE '>10次'
            END AS tc_user_app_visit_label,
            CASE 
                WHEN tc_visit_chan_label = '' OR tc_visit_chan_label IS NULL THEN 0
                ELSE SIZE(SPLIT(tc_visit_chan_label, '\\\\\\\\+')) 
            END AS tc_visit_chan_num, --- 访问渠道次数
            tc_visit_chan_label, --- 访问渠道
            get_json_object(path_info, '$.parent_sale_ord_id') AS parent_sale_ord_id,
            get_json_object(path_info, '$.visit_chan_cate2_cds_with_times') AS visit_chan_cate2_cds_with_times,
            get_json_object(path_info, '$.visit_chan_cate2_all_times') AS visit_chan_cate2_all_times,
            COALESCE(CAST(get_json_object(path_info, '$.after_prefr_amount_1') AS FLOAT), 0.0) AS after_prefr_amount_1,
            COALESCE(CAST(get_json_object(path_info, '$.before_first_order_visit_cnt') AS INT), 0) AS tc_user_visits_before_deal
        FROM
        (
            SELECT
                COALESCE(unif_user_log_acct, browser_uniq_id) AS user_id,
                MIN(attr) AS attr,
                COUNT(DISTINCT pv_id)  AS today_browse_times,
                regexp_replace(CONCAT_WS('+', sort_array(collect_set(chan_second_cate_desc_bel))),'(\\\\\\\\d)(-)','') AS tc_visit_chan_label,
                CONCAT('[', CONCAT_WS(',', collect_list(info)), ']') AS infos
            FROM
                userinfo_tab
            GROUP BY 
                COALESCE(unif_user_log_acct, browser_uniq_id)
        ) cc lateral VIEW explode(GetConversionPathNew(infos)) dd AS path_info
    ) path_tab
    ON COALESCE(pintab.unif_user_log_acct, pintab.browser_uniq_id) = path_tab.user_id
    LEFT JOIN
        (
            SELECT
                LOWER(TRIM(COALESCE(unif_user_log_acct, user_log_acct))) AS unif_user_log_acct,
                MAX(user_life_cycle_type) AS user_life_cycle_type
            FROM
                app.app_yhzz_hulk_user_insight
            WHERE
                dt = DATE_SUB('"""+yesterday+"""', 1)  
            GROUP BY 
                LOWER(TRIM(COALESCE(unif_user_log_acct, user_log_acct)))
        ) abcltab
    ON CASE WHEN pintab.unif_user_log_acct <> '' THEN pintab.unif_user_log_acct ELSE CONCAT('hive-', RAND()) END = abcltab.unif_user_log_acct
    LEFT JOIN
        (
            SELECT
                LOWER(TRIM(unif_user_log_acct)) AS unif_user_log_acct,
                MIN(init_acct_status) AS init_acct_status
            FROM
                app.app_traffic_center_pin_label_d_v2
            WHERE
                dt = '"""+yesterday+"""'  
            GROUP BY 
                LOWER(TRIM(unif_user_log_acct))
        ) loglabel
    ON CASE WHEN pintab.unif_user_log_acct <> '' THEN pintab.unif_user_log_acct ELSE CONCAT('hive-', RAND()) END = loglabel.unif_user_log_acct
    LEFT JOIN
        (
            SELECT
                LOWER(TRIM(unif_user_log_acct)) AS unif_user_log_acct, --- 归一pin
                MAX(user_visitcycle_app) AS pin_user_visitcycle_app, --- app访问周期
                MAX(user_buycycle_all) AS pin_user_buycycle_all --- 购买周期
            FROM
                app.app_traffic_center_pin_grid_d_v2 --- 带pin的用户标签表
            WHERE
                dt = DATE_SUB('"""+yesterday+"""', 1)  --- T+2 关联用户当前的生命周期
            GROUP BY 
                LOWER(TRIM(unif_user_log_acct))
        ) pin_user_label
    ON CASE WHEN pintab.unif_user_log_acct <> '' THEN pintab.unif_user_log_acct ELSE CONCAT('hive-', RAND()) END = pin_user_label.unif_user_log_acct    
    LEFT JOIN
        (
            SELECT
                browse_uniq_id, --- uuid
                MAX(user_visitcycle_app) AS uuid_user_visitcycle_app, --- app访问周期
                MAX(user_buycycle_all) AS uuid_user_buycycle_all --- 购买周期
            FROM
                app.app_traffic_center_uuid_grid_d_v2 --- 带uuid的用户标签表
            WHERE
                dt = DATE_SUB('"""+yesterday+"""', 1)  --- T+2 关联用户当前的生命周期
            GROUP BY 
                browse_uniq_id
        ) uuid_user_label
    ON CASE WHEN pintab.browser_uniq_id <> '' THEN pintab.browser_uniq_id ELSE CONCAT('hive-', RAND()) END = uuid_user_label.browse_uniq_id  
    ;
    """

    stable_history_sql = """
        ADD JAR hdfs://ns22019/user/mart_jd_ad/mart_jd_ad_sz/zhaoyinze/jd_ad_data_hive_udf-1.0-SNAPSHOT.jar;
        CREATE TEMPORARY FUNCTION IsDirtyDevice AS 'com.jd.ad.data.udf.IsDirtyDevice';
        CREATE TEMPORARY FUNCTION MapToStr AS 'com.jd.ad.data.udf.MapToStr';
        CREATE TEMPORARY FUNCTION ArrayToStr AS 'com.jd.ad.data.udf.ArrayToStr';
        CREATE TEMPORARY FUNCTION GetConversionPathNew AS 'com.jd.ad.data.udf.GetConversionPathNew';

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

        WITH orderdata_tab AS
        (
            SELECT
                unif_user_log_acct,
                user_log_acct,
                parent_sale_ord_id,
                sale_ord_tm AS even_time,
                SUM(after_prefr_amount_1) AS after_prefr_amount_1
            FROM 
                (
                    SELECT
                        LOWER(TRIM(unif_user_log_acct)) AS unif_user_log_acct,
                        LOWER(TRIM(user_log_acct)) AS user_log_acct,
                        item_sku_id,
                        sale_ord_id,
                        CASE WHEN type_c = 'C端' THEN parent_sale_ord_id ELSE '' END AS parent_sale_ord_id,
                        SPLIT(sale_ord_tm, '\\\\\\\\.')[0] AS sale_ord_tm,
                        MAX(CASE WHEN type_c = 'C端' THEN after_prefr_amount_1 ELSE 0.0 END) AS after_prefr_amount_1
                    FROM app.app_mkt_d01_retail_ltm_ord_sku_di
                    WHERE 
                        dt = '"""+quarter_first+""""' --- 和stable的区别
                        AND sale_ord_dt = '"""+yesterday+"""'
                        AND tp = 1 --- 区别
                        AND dp = 'QTD'
                        AND new_bs = 'APP'
                        AND intraday_ord_deal_flag = '1'
                    GROUP BY 
                        LOWER(TRIM(unif_user_log_acct)),
                        LOWER(TRIM(user_log_acct)),
                        item_sku_id,
                        sale_ord_id,
                        CASE WHEN type_c = 'C端' THEN parent_sale_ord_id ELSE '' END,
                        SPLIT(sale_ord_tm, '\\\\\\\\.')[0]
                ) innertab
            GROUP BY 
                unif_user_log_acct,
                user_log_acct,
                parent_sale_ord_id,
                sale_ord_tm
        ),
        userinfo_tab AS
        (
            SELECT
                unif_user_log_acct,
                user_log_acct,
                browser_uniq_id,
                attr,
                pv_id,
                --- 为单渠道添加优先级
                CASE
                    WHEN chan_second_cate_desc_bel = '直接流量' THEN CONCAT('1-',chan_second_cate_desc_bel) 
                    WHEN chan_second_cate_desc_bel = '搜索及信息流' THEN CONCAT('2-',chan_second_cate_desc_bel) 
                    WHEN chan_second_cate_desc_bel = '展示广告' THEN CONCAT('3-',chan_second_cate_desc_bel) 
                    WHEN chan_second_cate_desc_bel = '广告变现' THEN CONCAT('4-',chan_second_cate_desc_bel) 
                    WHEN chan_second_cate_desc_bel = '腾讯资源' THEN CONCAT('5-',chan_second_cate_desc_bel) 
                    WHEN chan_second_cate_desc_bel = '联盟' THEN CONCAT('6-',chan_second_cate_desc_bel) 
                    WHEN chan_second_cate_desc_bel = 'APP投放引流' THEN CONCAT('7-',chan_second_cate_desc_bel) 
                    WHEN chan_second_cate_desc_bel = '付费其他' THEN CONCAT('8-',chan_second_cate_desc_bel) 
                    WHEN chan_second_cate_desc_bel = '免费其他' THEN CONCAT('9-',chan_second_cate_desc_bel) 
                    ELSE NULL
                END AS chan_second_cate_desc_bel,
                MapToStr(MAP('event_time', even_time, 'event', attr, 'user_log_acct', COALESCE(unif_user_log_acct, browser_uniq_id), 'parent_sale_ord_id', parent_sale_ord_id, 'after_prefr_amount_1', after_prefr_amount_1, 'chan_second_cate_desc', NVL(chan_second_cate_desc_bel, ''), 'jdv', NVL(pv_id, ''))) AS info
            FROM
            (
                SELECT
                    unif_user_log_acct,
                    user_log_acct,
                    browser_uniq_id,
                    chan_second_cate_desc_bel,
                    pv_id,
                    '' AS parent_sale_ord_id,
                    request_tm AS even_time,
                    0 AS after_prefr_amount_1,
                    'visit' AS attr
                FROM
                    app.app_tc_sch_d14_traffic_plat_d
                WHERE 
                    dt = '"""+yesterday+"""'
                    AND platform = 'APP'

                UNION ALL 

                SELECT
                    unif_user_log_acct,
                    user_log_acct,
                    NULL AS browser_uniq_id,
                    NULL AS chan_second_cate_desc_bel,
                    NULL AS pv_id,
                    parent_sale_ord_id,
                    even_time,
                    after_prefr_amount_1,
                    'order' AS attr
                FROM 
                    orderdata_tab
            ) aa
        )

        INSERT OVERWRITE TABLE app.app_tc_visitpath_user_detail_report_dt PARTITION(dt='"""+yesterday+"""',tp,dp) 
        SELECT
            'APP' AS platform,   
            pintab.unif_user_log_acct AS unif_user_log_acct,
            user_log_acct,
            browser_uniq_id,
            attr,
            CASE  WHEN (pintab.unif_user_log_acct IS NULL OR init_acct_status = '0') THEN '无pin'
                    WHEN pintab.unif_user_log_acct <> '' AND (user_life_cycle_type IS NULL OR user_life_cycle_type = '-1') THEN '生命周期未知(有pin)'  
            ELSE user_life_cycle_type END AS user_life_cycle_type,
            parent_sale_ord_id,
            CASE 
                WHEN tc_visit_chan_num = 0 THEN ''
                WHEN tc_visit_chan_num <= 4 THEN tc_visit_chan_label
                ELSE '其他' 
            END AS tc_visit_chan,
            tc_visit_chan_num,
            visit_chan_cate2_cds_with_times,
            tc_user_app_visit_label AS user_visit_freq,
            today_browse_times AS tc_visit_times,
            tc_user_visits_before_deal,
            after_prefr_amount_1 AS tc_deal_after_prefr_amount_1,
            CASE WHEN
                    (
                        visit_chan_cate2_all_times = 0
                        OR visit_chan_cate2_all_times IS NULL
                    )
                THEN '0次'
                WHEN visit_chan_cate2_all_times < 7
                THEN CONCAT(CAST(visit_chan_cate2_all_times AS string), '次')
                WHEN visit_chan_cate2_all_times < 11
                THEN '[7-10]次'
                ELSE '>10次'
            END AS path_user_visit_freq,
            COALESCE(pin_user_visitcycle_app, uuid_user_visitcycle_app, '近365日未访') AS user_visitcycle, --- 访问周期
            COALESCE(pin_user_buycycle_all, uuid_user_buycycle_all, '历史未购') AS user_buycycle, --- 购买周期
            'day' AS tp,
            'stable' AS dp
        FROM
        (
            SELECT
                unif_user_log_acct,
                user_log_acct,
                browser_uniq_id
            FROM
                userinfo_tab
            GROUP BY
                browser_uniq_id,
                unif_user_log_acct,
                user_log_acct
        ) pintab
        LEFT JOIN
        (
            SELECT
                user_id,
                attr,
                today_browse_times,
                CASE
                    WHEN
                        (
                            today_browse_times = 0
                            OR today_browse_times IS NULL
                        )
                    THEN '0次'
                    WHEN today_browse_times < 7
                    THEN CONCAT(CAST(today_browse_times AS string), '次')
                    WHEN today_browse_times < 11
                    THEN '[7-10]次'
                    ELSE '>10次'
                END AS tc_user_app_visit_label,
                CASE 
                    WHEN tc_visit_chan_label = '' OR tc_visit_chan_label IS NULL THEN 0
                    ELSE SIZE(SPLIT(tc_visit_chan_label, '\\\\\\\\+')) 
                END AS tc_visit_chan_num,
                --- === 修改 ===
                tc_visit_chan_label,
                get_json_object(path_info, '$.parent_sale_ord_id') AS parent_sale_ord_id,
                get_json_object(path_info, '$.visit_chan_cate2_cds_with_times') AS visit_chan_cate2_cds_with_times,
                get_json_object(path_info, '$.visit_chan_cate2_all_times') AS visit_chan_cate2_all_times,
                COALESCE(CAST(get_json_object(path_info, '$.after_prefr_amount_1') AS FLOAT), 0.0) AS after_prefr_amount_1,
                COALESCE(CAST(get_json_object(path_info, '$.before_first_order_visit_cnt') AS INT), 0) AS tc_user_visits_before_deal
            FROM
            (
                SELECT
                    COALESCE(unif_user_log_acct, browser_uniq_id) AS user_id,
                    MIN(attr) AS attr,
                    COUNT(DISTINCT pv_id)  AS today_browse_times,
                    regexp_replace(CONCAT_WS('+', sort_array(collect_set(chan_second_cate_desc_bel))),'(\\\\\\\\d)(-)','') AS tc_visit_chan_label,
                    CONCAT('[', CONCAT_WS(',', collect_list(info)), ']') AS infos
                FROM
                    userinfo_tab
                GROUP BY 
                    COALESCE(unif_user_log_acct, browser_uniq_id)
            ) cc lateral VIEW explode(GetConversionPathNew(infos)) dd AS path_info
        ) path_tab
        ON COALESCE(pintab.unif_user_log_acct, pintab.browser_uniq_id) = path_tab.user_id
        LEFT JOIN
            (
                SELECT
                    LOWER(TRIM(COALESCE(unif_user_log_acct, user_log_acct))) AS unif_user_log_acct,
                    MAX(user_life_cycle_type) AS user_life_cycle_type
                FROM
                    app.app_yhzz_hulk_user_insight
                WHERE
                    dt = DATE_SUB('"""+yesterday+"""', 1)
                GROUP BY 
                    LOWER(TRIM(COALESCE(unif_user_log_acct, user_log_acct)))
            ) abcltab
        ON CASE WHEN pintab.unif_user_log_acct <> '' THEN pintab.unif_user_log_acct ELSE CONCAT('hive-', RAND()) END = abcltab.unif_user_log_acct
        LEFT JOIN
            (
                SELECT
                    LOWER(TRIM(unif_user_log_acct)) AS unif_user_log_acct,
                    MIN(init_acct_status) AS init_acct_status
                FROM
                    app.app_traffic_center_pin_label_d_v2
                WHERE
                    dt = '"""+yesterday+"""'
                GROUP BY 
                    LOWER(TRIM(unif_user_log_acct))
            ) loglabel
        ON CASE WHEN pintab.unif_user_log_acct <> '' THEN pintab.unif_user_log_acct ELSE CONCAT('hive-', RAND()) END = loglabel.unif_user_log_acct
        LEFT JOIN
            (
                SELECT
                    LOWER(TRIM(unif_user_log_acct)) AS unif_user_log_acct, --- 归一pin
                    MAX(user_visitcycle_app) AS pin_user_visitcycle_app, --- app访问周期
                    MAX(user_buycycle_all) AS pin_user_buycycle_all --- 购买周期
                FROM
                    app.app_traffic_center_pin_grid_d_v2 --- 带pin的用户标签表
                WHERE
                    dt = DATE_SUB('"""+yesterday+"""', 1)  --- T+2 关联用户当前的生命周期
                GROUP BY 
                    LOWER(TRIM(unif_user_log_acct))
            ) pin_user_label
        ON CASE WHEN pintab.unif_user_log_acct <> '' THEN pintab.unif_user_log_acct ELSE CONCAT('hive-', RAND()) END = pin_user_label.unif_user_log_acct    
        LEFT JOIN
            (
                SELECT
                    browse_uniq_id, --- uuid
                    MAX(user_visitcycle_app) AS uuid_user_visitcycle_app, --- app访问周期
                    MAX(user_buycycle_all) AS uuid_user_buycycle_all --- 购买周期
                FROM
                    app.app_traffic_center_uuid_grid_d_v2 --- 带uuid的用户标签表
                WHERE
                    dt = DATE_SUB('"""+yesterday+"""', 1)  --- T+2 关联用户当前的生命周期
                GROUP BY 
                    browse_uniq_id
            ) uuid_user_label
        ON CASE WHEN pintab.browser_uniq_id <> '' THEN pintab.browser_uniq_id ELSE CONCAT('hive-', RAND()) END = uuid_user_label.browse_uniq_id  
        ;
    """

    snapshot_sql = """
    ADD JAR hdfs://ns22019/user/mart_jd_ad/mart_jd_ad_sz/zhaoyinze/jd_ad_data_hive_udf-1.0-SNAPSHOT.jar;
    CREATE TEMPORARY FUNCTION IsDirtyDevice AS 'com.jd.ad.data.udf.IsDirtyDevice';
    CREATE TEMPORARY FUNCTION MapToStr AS 'com.jd.ad.data.udf.MapToStr';
    CREATE TEMPORARY FUNCTION ArrayToStr AS 'com.jd.ad.data.udf.ArrayToStr';
    CREATE TEMPORARY FUNCTION GetConversionPathNew AS 'com.jd.ad.data.udf.GetConversionPathNew';
    
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
    
    WITH orderdata_tab AS
    (
        SELECT
            COALESCE(norm.unif_user_log_acct, deal_tab.user_log_acct) AS unif_user_log_acct,
            deal_tab.user_log_acct AS user_log_acct,
            CASE WHEN toc_label.sale_ord_id IS NOT NULL THEN parent_sale_ord_id --C端
                ELSE ''
            END AS parent_sale_ord_id,
            sale_ord_tm AS even_time,
            SUM(CASE WHEN toc_label.sale_ord_id IS NOT NULL THEN after_prefr_amount --C端
                ELSE 0.0 
            END) AS after_prefr_amount_1
        FROM
            (
                SELECT
                    LOWER(TRIM(user_log_acct)) AS user_log_acct,
                    item_sku_id,
                    sale_ord_id,
                    parent_sale_ord_id,
                    SPLIT(sale_ord_tm, '\\\\\\\\.') [0] AS sale_ord_tm,
                    MAX(after_prefr_amount) AS after_prefr_amount
                FROM
                    app.app_m14_allplat_chan_cart_ord_det_d --- (全渠道订单跟单模型结果表) 区别 ？app_mkt_d01_retail_ltm_ord_sku_di(零售LTM-基础数据模型-T+2)
                WHERE
                    dt = '"""+yesterday+"""'
                    AND add_cart_plat_flag NOT IN('YHD_PC', 'YHD_M', 'YHD_APP')
                    AND plat_flag = 'APP'
                    AND deal_flag = '1'
                GROUP BY
                    LOWER(TRIM(user_log_acct)),
                    item_sku_id,
                    sale_ord_id,
                    parent_sale_ord_id,
                    SPLIT(sale_ord_tm, '\\\\\\\\.') [0]
            )
            deal_tab
        LEFT JOIN
            (
                SELECT
                    sale_ord_id,
                    item_sku_id
                FROM
                    adm.adm_d04_trade_ord_bc_classify_di --- 订单标签模型，主要是BC分类，关联使用
                WHERE
                    dt = '"""+yesterday+"""'
                    AND first_biz IN('TOC_零售')
                GROUP BY
                    sale_ord_id,
                    item_sku_id
            )
            toc_label
        ON
            deal_tab.sale_ord_id = toc_label.sale_ord_id
            AND deal_tab.item_sku_id = toc_label.item_sku_id
        LEFT JOIN
            (
                SELECT
                    LOWER(TRIM(user_acct_name)) AS user_acct_name,
                    MAX(LOWER(TRIM(unif_user_log_acct))) AS unif_user_log_acct
                FROM
                    gdm.gdm_m01_userinfo_basic_da --- 用户信息
                WHERE
                    dt = '"""+yesterday+"""'
                    AND user_acct_name <> unif_user_log_acct
                GROUP BY
                    LOWER(TRIM(user_acct_name))
            )
            norm
        ON deal_tab.user_log_acct = norm.user_acct_name
        GROUP BY
            COALESCE(norm.unif_user_log_acct, deal_tab.user_log_acct),
            deal_tab.user_log_acct,
            CASE WHEN toc_label.sale_ord_id IS NOT NULL THEN parent_sale_ord_id ELSE '' END,
            sale_ord_tm
    ),
    userinfo_tab AS
    (
        SELECT
            unif_user_log_acct,
            user_log_acct,
            browser_uniq_id,
            attr,
            pv_id,
            --- 为单渠道添加优先级
            CASE
                WHEN chan_second_cate_desc_bel = '直接流量' THEN CONCAT('1-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '搜索及信息流' THEN CONCAT('2-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '展示广告' THEN CONCAT('3-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '广告变现' THEN CONCAT('4-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '腾讯资源' THEN CONCAT('5-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '联盟' THEN CONCAT('6-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = 'APP投放引流' THEN CONCAT('7-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '付费其他' THEN CONCAT('8-',chan_second_cate_desc_bel) 
                WHEN chan_second_cate_desc_bel = '免费其他' THEN CONCAT('9-',chan_second_cate_desc_bel) 
                ELSE NULL
            END AS chan_second_cate_desc_bel,
            MapToStr(MAP('event_time', even_time, 'event', attr, 'user_log_acct', COALESCE(unif_user_log_acct, browser_uniq_id), 'parent_sale_ord_id', parent_sale_ord_id, 'after_prefr_amount_1', after_prefr_amount_1, 'chan_second_cate_desc', NVL(chan_second_cate_desc_bel, ''), 'jdv', NVL(pv_id, ''))) AS info
        FROM
        (
            SELECT
                unif_user_log_acct,
                user_log_acct,
                browser_uniq_id,
                chan_second_cate_desc_bel,
                pv_id,
                '' AS parent_sale_ord_id,
                request_tm AS even_time,
                0 AS after_prefr_amount_1,
                'visit' AS attr
            FROM
                app.app_tc_sch_d14_traffic_plat_d
            WHERE 
                dt = '"""+yesterday+"""'
                AND platform = 'APP'
                
            UNION ALL 
            
            SELECT
                unif_user_log_acct,
                user_log_acct,
                NULL AS browser_uniq_id,
                NULL AS chan_second_cate_desc_bel,
                NULL AS pv_id,
                parent_sale_ord_id,
                even_time,
                after_prefr_amount_1,
                'order' AS attr
            FROM 
                orderdata_tab --- 快照的订单数据来源表不一样
        ) aa
    )
    
    INSERT OVERWRITE TABLE app.app_tc_visitpath_user_detail_report_dt PARTITION(dt='"""+yesterday+"""',tp,dp) 
    SELECT
        'APP' AS platform,   
        pintab.unif_user_log_acct AS unif_user_log_acct,
        user_log_acct,
        browser_uniq_id,
        attr,
        CASE  WHEN (pintab.unif_user_log_acct IS NULL OR init_acct_status = '0') THEN '无pin'
                WHEN pintab.unif_user_log_acct <> '' AND (user_life_cycle_type IS NULL OR user_life_cycle_type = '-1') THEN '生命周期未知(有pin)'  
        ELSE user_life_cycle_type END AS user_life_cycle_type,
        parent_sale_ord_id,
        CASE 
            WHEN tc_visit_chan_num = 0 THEN ''
            WHEN tc_visit_chan_num <= 4 THEN tc_visit_chan_label
            ELSE '其他组合' 
        END AS tc_visit_chan,
        tc_visit_chan_num,
        visit_chan_cate2_cds_with_times,
        tc_user_app_visit_label AS user_visit_freq,
        today_browse_times AS tc_visit_times,
        tc_user_visits_before_deal,
        after_prefr_amount_1 AS tc_deal_after_prefr_amount_1,
        CASE WHEN
                (
                    visit_chan_cate2_all_times = 0
                    OR visit_chan_cate2_all_times IS NULL
                )
            THEN '0次'
            WHEN visit_chan_cate2_all_times < 7
            THEN CONCAT(CAST(visit_chan_cate2_all_times AS string), '次')
            WHEN visit_chan_cate2_all_times < 11
            THEN '[7-10]次'
            ELSE '>10次'
        END AS path_user_visit_freq,
        COALESCE(pin_user_visitcycle_app, uuid_user_visitcycle_app, '近365日未访') AS user_visitcycle, --- 访问周期
        COALESCE(pin_user_buycycle_all, uuid_user_buycycle_all, '历史未购') AS user_buycycle, --- 购买周期
        'day' AS tp,
        'snapshot' AS dp
    FROM
    (
        SELECT
            unif_user_log_acct,
            user_log_acct,
            browser_uniq_id
        FROM
            userinfo_tab
        GROUP BY
            browser_uniq_id,
            unif_user_log_acct,
            user_log_acct
    ) pintab
    LEFT JOIN
    (
        SELECT
            user_id,
            attr,
            today_browse_times,
            CASE
                WHEN
                    (
                        today_browse_times = 0
                        OR today_browse_times IS NULL
                    )
                THEN '0次'
                WHEN today_browse_times < 7
                THEN CONCAT(CAST(today_browse_times AS string), '次')
                WHEN today_browse_times < 11
                THEN '[7-10]次'
                ELSE '>10次'
            END AS tc_user_app_visit_label,
            CASE --- 只有订单没有浏览，底层数据归因问题 
                WHEN tc_visit_chan_label = '' OR tc_visit_chan_label IS NULL THEN 0
                ELSE SIZE(SPLIT(tc_visit_chan_label, '\\\\\\\\+')) 
            END AS tc_visit_chan_num,
            tc_visit_chan_label,
            get_json_object(path_info, '$.parent_sale_ord_id') AS parent_sale_ord_id,
            get_json_object(path_info, '$.visit_chan_cate2_cds_with_times') AS visit_chan_cate2_cds_with_times,
            get_json_object(path_info, '$.visit_chan_cate2_all_times') AS visit_chan_cate2_all_times,
            COALESCE(CAST(get_json_object(path_info, '$.after_prefr_amount_1') AS FLOAT), 0.0) AS after_prefr_amount_1,
            COALESCE(CAST(get_json_object(path_info, '$.before_first_order_visit_cnt') AS INT), 0) AS tc_user_visits_before_deal
        FROM
        (
            SELECT
                COALESCE(unif_user_log_acct, browser_uniq_id) AS user_id,
                MIN(attr) AS attr,
                COUNT(DISTINCT pv_id)  AS today_browse_times,
                regexp_replace(CONCAT_WS('+', sort_array(collect_set(chan_second_cate_desc_bel))),'(\\\\\\\\d)(-)','') AS tc_visit_chan_label,
                CONCAT('[', CONCAT_WS(',', collect_list(info)), ']') AS infos
            FROM
                userinfo_tab
            GROUP BY 
                COALESCE(unif_user_log_acct, browser_uniq_id)
        ) cc lateral VIEW explode(GetConversionPathNew(infos)) dd AS path_info
    ) path_tab
    ON COALESCE(pintab.unif_user_log_acct, pintab.browser_uniq_id) = path_tab.user_id
    LEFT JOIN
        (
            SELECT
                LOWER(TRIM(COALESCE(unif_user_log_acct, user_log_acct))) AS unif_user_log_acct,
                MAX(user_life_cycle_type) AS user_life_cycle_type
            FROM
                app.app_yhzz_hulk_user_insight
            WHERE
                dt = DATE_SUB('"""+yesterday+"""', 1)
            GROUP BY 
                LOWER(TRIM(COALESCE(unif_user_log_acct, user_log_acct)))
        ) abcltab
    ON CASE WHEN pintab.unif_user_log_acct <> '' THEN pintab.unif_user_log_acct ELSE CONCAT('hive-', RAND()) END = abcltab.unif_user_log_acct
    LEFT JOIN
        (
            SELECT
                LOWER(TRIM(unif_user_log_acct)) AS unif_user_log_acct,
                MIN(init_acct_status) AS init_acct_status
            FROM
                app.app_traffic_center_pin_label_d_v2
            WHERE
                dt = '"""+yesterday+"""'
            GROUP BY 
                LOWER(TRIM(unif_user_log_acct))
        ) loglabel
    ON CASE WHEN pintab.unif_user_log_acct <> '' THEN pintab.unif_user_log_acct ELSE CONCAT('hive-', RAND()) END = loglabel.unif_user_log_acct
    LEFT JOIN
        (
            SELECT
                LOWER(TRIM(unif_user_log_acct)) AS unif_user_log_acct, --- 归一pin
                MAX(user_visitcycle_app) AS pin_user_visitcycle_app, --- app访问周期
                MAX(user_buycycle_all) AS pin_user_buycycle_all --- 购买周期
            FROM
                app.app_traffic_center_pin_grid_d_v2 --- 带pin的用户标签表
            WHERE
                dt = DATE_SUB('"""+yesterday+"""', 1)  --- T+2 关联用户当前的生命周期
            GROUP BY 
                LOWER(TRIM(unif_user_log_acct))
        ) pin_user_label
    ON CASE WHEN pintab.unif_user_log_acct <> '' THEN pintab.unif_user_log_acct ELSE CONCAT('hive-', RAND()) END = pin_user_label.unif_user_log_acct    
    LEFT JOIN
        (
            SELECT
                browse_uniq_id, --- uuid
                MAX(user_visitcycle_app) AS uuid_user_visitcycle_app, --- app访问周期
                MAX(user_buycycle_all) AS uuid_user_buycycle_all --- 购买周期
            FROM
                app.app_traffic_center_uuid_grid_d_v2 --- 带uuid的用户标签表
            WHERE
                dt = DATE_SUB('"""+yesterday+"""', 1)  --- T+2 关联用户当前的生命周期
            GROUP BY 
                browse_uniq_id
        ) uuid_user_label
    ON CASE WHEN pintab.browser_uniq_id <> '' THEN pintab.browser_uniq_id ELSE CONCAT('hive-', RAND()) END = uuid_user_label.browse_uniq_id  
    ;
    """

    if dp == 'stable':
        sql = stable_sql
    elif dp == 'snapshot':
        sql = snapshot_sql
    elif dp == 'history':
        sql = stable_history_sql
    else:
        raise EOFError('请指定dp=stable或者dp=snapshot.')
    ht.exec_sql(schema_name='app', table_name='app_tc_visitpath_user_detail_report_dt', sql=sql,
                exec_engine='spark',
                spark_resource_level='high', spark_args=['--conf spark.sql.hive.mergeFiles=true'])
    # ftime + 1
    ftime = (datetime.datetime.strptime(ftime, '%Y%m%d') + datetime.timedelta(days=1)).strftime('%Y%m%d')
print("FixTask Has Finished.")
