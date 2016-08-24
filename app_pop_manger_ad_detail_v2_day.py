#!/usr/bin/env python3
#===============================================================================
#
#         FILE: app_pop_manger_ad_detail_v2_day.py
#
#        USAGE: ./app_pop_manger_ad_detail_v2_day.py
#
#  DESCRIPTION: 
#
#      OPTIONS: ---
# REQUIREMENTS: ---
#         BUGS: ---
#        NOTES: 模式这块，不能直接判断自营还是pop，因为有些数据没有sku_id，所以只能先判断出来自营(1)，然后把其他的都置为pop即为2
#       AUTHOR: liuyuhua@jd.com
#      COMPANY: jd.com
#      VERSION: 1.0
#      CREATED: 20160726
#    SRC_TABLE: 
#    TGT_TABLE: app_pop_manger_ad_detail_v2_day
#===============================================================================
import sys
import os
sys.path.append(os.getenv('HIVE_TASK'))
from HiveTask import HiveTask

ht = HiveTask()
sql = """
use app;

set mapred.output.compress=true; 
set hive.exec.compress.output=true; 
set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec; 

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table app_pop_manger_ad_detail_v2_day partition(dt = '""" + ht.data_day_str + """')
select
   ftime                            as op_time                     
  ,site_id                          as site_id  
  ,adspec_id                        as adspec_id
  ,adpos_id                         as adpos_id
  ,jd_pin                           as jd_pin
  ,ad_id                            as ad_id  
  ,(case when getDataTypeBySkuId(cast(scomm_id as bigint)) in(-1,-5,-6,0,10,11,12,13,14,15) then jd_supplierid else jd_vendor end) as advertiser_id 
  ,scomm_id                         as item_sku_id
  ,(case when getDataTypeBySkuId(cast(scomm_id as bigint)) in(-1,-5,-6,0,10,11,12,13,14,15) then 1 else 2 end) as mode_type
  ,sum(expose_nums                ) as expose_nums                
  ,sum(effect_click_nums          ) as effect_click_nums          
  ,sum(realcost_money             ) as realcost_money             
  ,sum(realcost_gdt_cash_money    ) as realcost_gdt_cash_money    
  ,sum(realcost_gdt_virt_money    ) as realcost_gdt_virt_money    
  ,sum(realcost_gdt_wireless_money) as realcost_gdt_wireless_money
  ,sum(realcost_subsidy_money     ) as realcost_subsidy_money     
  ,sum(gen_dir_nums               ) as gen_dir_nums               
  ,sum(gen_dir_money              ) as gen_dir_money              
  ,sum(pay_dir_nums               ) as pay_dir_nums               
  ,sum(pay_dir_money              ) as pay_dir_money              
  ,sum(gen_rel_nums               ) as gen_rel_nums               
  ,sum(gen_rel_money              ) as gen_rel_money              
  ,sum(pay_rel_nums               ) as pay_rel_nums               
  ,sum(pay_rel_money              ) as pay_rel_money              
  ,sum(gen_pla_nums               ) as gen_pla_nums               
  ,sum(gen_pla_money              ) as gen_pla_money              
  ,sum(pay_pla_nums               ) as pay_pla_nums               
  ,sum(pay_pla_money              ) as pay_pla_money              
  ,sum(gen_dir_nums_15d           ) as gen_dir_nums_15d           
  ,sum(gen_dir_money_15d          ) as gen_dir_money_15d          
  ,sum(pay_dir_nums_15d           ) as pay_dir_nums_15d           
  ,sum(pay_dir_money_15d          ) as pay_dir_money_15d          
  ,sum(gen_rel_nums_15d           ) as gen_rel_nums_15d           
  ,sum(gen_rel_money_15d          ) as gen_rel_money_15d          
  ,sum(pay_rel_nums_15d           ) as pay_rel_nums_15d           
  ,sum(pay_rel_money_15d          ) as pay_rel_money_15d          
  ,sum(gen_pla_nums_15d           ) as gen_pla_nums_15d           
  ,sum(gen_pla_money_15d          ) as gen_pla_money_15d          
  ,sum(pay_pla_nums_15d           ) as pay_pla_nums_15d           
  ,sum(pay_pla_money_15d          ) as pay_pla_money_15d    
from app.app_szad_w_ads_jd_ad_detail_fsjj_day
where ftime = '""" + ht.data_day_str + """'
group by 
   ftime                        
  ,site_id    
  ,adspec_id  
  ,adpos_id   
  ,jd_pin     
  ,ad_id      
  ,(case when getDataTypeBySkuId(cast(scomm_id as bigint)) in(-1,-5,-6,0,10,11,12,13,14,15) then jd_supplierid else jd_vendor end)                   
  ,scomm_id    
  ,(case when getDataTypeBySkuId(cast(scomm_id as bigint)) in(-1,-5,-6,0,10,11,12,13,14,15) then 1 else 2 end)              
;
"""

ht.exec_sql(schema_name = 'app', table_name = 'app_pop_manger_ad_detail_v2_day', sql = sql, merge_flag = True, merge_part_dir = ['dt=' + ht.data_day_str + '']) 

#==============================================================================================
#   schema_name: 必选
#    table_name: 可选
#           sql: 必选
#    merge_flag: False (default)
#  lzo_compress: 可选 False (default)  
#lzo_index_path: 依赖lzo_compress可选，不需要warehouse，实例化了表后自动找到localtion
#                '' ,[''] 压缩整个表 
#                Normal,
#                /home/use/dd_edw/db/table
#                ['partition1','partition2']
#                ['dir1','dir2']
#               
#merge_part_dir: [](default) 整个表都检测小文件  
#                [partition1,partition2]
#      min_size: 128Mb
#----------------------------------------------------------------------------------------------
#      max_size: 250Mb
#---------------------------------------------------------------------------------------------
#ht.merge_small_file(db, table, partition = [], min_size = 128*1024*1024)
#ht.CreateIndex(db, table, path = 'Normal')
#===============================================================================================