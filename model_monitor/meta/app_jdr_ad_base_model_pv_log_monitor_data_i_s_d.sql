CREATE EXTERNAL TABLE `app.app_jdr_ad_base_model_pv_log_monitor_data_i_s_d`(

 `pv_chan_first_cate_cd` string COMMENT '一级渠道',
 `pv_chan_second_cate_cd` string COMMENT '二级渠道',
 `pv_chan_third_cate_cd` string COMMENT '三级渠道',
 `pv_chan_fourth_cate_cd` string COMMENT '四级渠道',

  --2.2.2 四级渠道统计浏览UV与计费点击关联统计浏览PV diff监控
  `pv` double COMMENT '总浏览量',

  `join_click_pv` double COMMENT '关联上点击的浏览量',

   --2.2.3 浏览表中jdv_utm_term中是否有宏替换失败的情况
  `jdv_click_id_macro_replace_faild` double COMMENT '宏替换失败浏览量'
)
COMMENT '渠道归因监控数据表'
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
  'hdfs://ns22019/user/mart_jd_ad/mart_jd_ad_sz/app.db/app_jdr_ad_base_model_pv_log_monitor_data_i_s_d'
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