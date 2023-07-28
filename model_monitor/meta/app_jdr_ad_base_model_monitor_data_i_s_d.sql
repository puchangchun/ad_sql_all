CREATE EXTERNAL TABLE `app.app_jdr_ad_base_model_monitor_data_i_s_d`(

  `ad_traffic_type` string COMMENT 'att（渠道类型）',
  `combine_play_type` string COMMENT '播放模式',

  --1.1.1 广告位为空明细层监控
  `all_clicks` double COMMENT '站外总点击量',
  `placementid_isnull_clicks` double COMMENT '站外点击中广告位为空点击量',

   --1.1.2 广告位为空报表层消耗监控
  `consumption` double COMMENT '总消耗',
  `placementid_isnull_consumption` double COMMENT '广告位为空消耗',

  --1.2 中间页二跳对应上一跳广告位id是否为空监控
  `repage_finance_clicks` double COMMENT '中间页的总计费点击量',
  `repage_last_placementid_isnull_finance_clicks` double COMMENT '中间页上一跳广告位为空的计费点击量',

   --1.3 点击日志中click_id为空监控，总计费点击是否跟上面的合并，
   `finance_clicks` double COMMENT '计费点击量',
   `clickid_isnull_finance_clicks` double COMMENT '计费点击中点击id为空点击量',

    --2.1 计费点击统计数与明细数diff监控
   `summary_finance_clicks` double COMMENT '汇总的计费点击统计数',
   `detail_finance_clicks` double COMMENT '明细的计费点击数',

   -- 2.2 计费分区点击明细与c2s点击关联比例稳定性监控（c2s和rtb的关联key怎么来的，逻辑怎么无确定的）
   `detail_rtb_join_c2s_clicks` double COMMENT '点击表rtb分区关联上c2s点击数',
   `detail_rtb_clicks` double COMMENT '点击表rtb分区点击数', --- 是否是计费点击

   --2.3 s2s与c2s点击的diff监控
   `detail_s2s_clicks` double COMMENT '点击表s2s点击数',
   `detail_c2s_clicks` double COMMENT '点击表c2s点击数',

   --2.4 计费点击明细统计消耗与网关回传单元粒度消耗 diff监控
  `summary_cpc_consumption` double COMMENT '宽表cpc消耗',
  `detail_cpc_consumption` double COMMENT '点击表cpc消耗'

)
COMMENT '基础模型监控数据表'
PARTITIONED BY (
  `dt` string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ns22019/user/mart_jd_ad/mart_jd_ad_sz/app.db/app_jdr_ad_base_model_monitor_data_i_s_d'
TBLPROPERTIES (
  'SENSITIVE_TABLE'='FALSE',
  'jdhive_global_idc'='mysql_hope',
  'jdhive_ms_idc'='ms-hope',
  'jdhive_storage_policy'='htyd,lfrz',
  'last_modified_by'='mart_jd_ad_sz',
  'last_modified_time'='1669270649',
  'mart_name'='jd_ad',
  'row_policy'='',
  'transient_lastDdlTime'='1680591822')