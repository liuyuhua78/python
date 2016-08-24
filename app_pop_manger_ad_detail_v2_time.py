#!/usr/bin/env python3
#===============================================================================
#
#         FILE: app_pop_manger_ad_detail_v2_time.py
#
#        USAGE: ./app_pop_manger_ad_detail_v2_time.py
#
#  DESCRIPTION: 
#
#      OPTIONS: ---
# REQUIREMENTS: ---
#         BUGS: ---
#        NOTES: 
#       AUTHOR: liuyuhua@jd.com
#      COMPANY: jd.com
#      VERSION: 1.0
#      CREATED: 20160726
#    SRC_TABLE: 
#    TGT_TABLE: app_pop_manger_ad_detail_v2_time
#===============================================================================
import sys
import os
sys.path.append(os.getenv('HIVE_TASK'))
from HiveTask import HiveTask

ht = HiveTask()
year_this    = ht.calendar.Year
year_before  = year_this - 1
year_after   = year_this + 1
year_this_77 = str(str(year_this) + "-77")
year_before_77 = str(str(year_before) + "-77")
print("year_this = "      + str(year_this))
print("year_before = "    + str(year_before))
print("year_this_77 = "   + str(year_this_77))
print("year_before_77 = " + str(year_before_77))

this_year    = str(year_this)
after_year   = str(year_this + 1)

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

--插入分天事实表
insert overwrite table app_pop_manger_ad_detail_v2_time partition(tp = 'day', dt)
select
   op_time                          as op_time         
  ,site_id                          as site_id      
  ,adspec_id                        as adspec_id    
  ,adpos_id                         as adpos_id     
  ,jd_pin                           as jd_pin       
  ,ad_id                            as ad_id        
  ,advertiser_id                    as advertiser_id
  ,item_sku_id                      as item_sku_id  
  ,mode_type                        as mode_type    
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
  ,op_time dt
from app.app_pop_manger_ad_detail_v2_day where dt = '""" + ht.data_day_str + """'
group by 
   op_time             
  ,site_id             
  ,adspec_id           
  ,adpos_id            
  ,jd_pin              
  ,ad_id               
  ,advertiser_id       
  ,item_sku_id         
  ,mode_type   
;

--插入近7天事实表
alter table app_pop_manger_ad_detail_v2_time drop partition(tp = 'week', dt = '""" + year_before_77 + """');
insert overwrite table app_pop_manger_ad_detail_v2_time partition(tp = 'week', dt)
select
   '""" + year_this_77 + """'       as op_time  
  ,site_id                          as site_id      
  ,adspec_id                        as adspec_id    
  ,adpos_id                         as adpos_id     
  ,jd_pin                           as jd_pin       
  ,ad_id                            as ad_id        
  ,advertiser_id                    as advertiser_id
  ,item_sku_id                      as item_sku_id  
  ,mode_type                        as mode_type    
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
  ,'""" + year_this_77 + """' as dt
from app.app_pop_manger_ad_detail_v2_day where dt >= sysdate(-7) and dt <= sysdate(-1)
group by 
   '""" + year_this_77 + """'   
  ,site_id             
  ,adspec_id           
  ,adpos_id            
  ,jd_pin              
  ,ad_id               
  ,advertiser_id       
  ,item_sku_id         
  ,mode_type   
;

--插入分周事实表
insert overwrite table app_pop_manger_ad_detail_v2_time partition(tp = 'week', dt)
select
   concat(substr(t2.dim_week_id, 1, 4), '-', substr(t2.dim_week_id, 5, 2)) as op_time  
  ,t1.site_id                          as site_id      
  ,t1.adspec_id                        as adspec_id    
  ,t1.adpos_id                         as adpos_id     
  ,t1.jd_pin                           as jd_pin       
  ,t1.ad_id                            as ad_id        
  ,t1.advertiser_id                    as advertiser_id
  ,t1.item_sku_id                      as item_sku_id  
  ,t1.mode_type                        as mode_type    
  ,sum(t1.expose_nums                ) as expose_nums                
  ,sum(t1.effect_click_nums          ) as effect_click_nums          
  ,sum(t1.realcost_money             ) as realcost_money             
  ,sum(t1.realcost_gdt_cash_money    ) as realcost_gdt_cash_money    
  ,sum(t1.realcost_gdt_virt_money    ) as realcost_gdt_virt_money    
  ,sum(t1.realcost_gdt_wireless_money) as realcost_gdt_wireless_money
  ,sum(t1.realcost_subsidy_money     ) as realcost_subsidy_money     
  ,sum(t1.gen_dir_nums               ) as gen_dir_nums               
  ,sum(t1.gen_dir_money              ) as gen_dir_money              
  ,sum(t1.pay_dir_nums               ) as pay_dir_nums               
  ,sum(t1.pay_dir_money              ) as pay_dir_money              
  ,sum(t1.gen_rel_nums               ) as gen_rel_nums               
  ,sum(t1.gen_rel_money              ) as gen_rel_money              
  ,sum(t1.pay_rel_nums               ) as pay_rel_nums               
  ,sum(t1.pay_rel_money              ) as pay_rel_money              
  ,sum(t1.gen_pla_nums               ) as gen_pla_nums               
  ,sum(t1.gen_pla_money              ) as gen_pla_money              
  ,sum(t1.pay_pla_nums               ) as pay_pla_nums               
  ,sum(t1.pay_pla_money              ) as pay_pla_money              
  ,sum(t1.gen_dir_nums_15d           ) as gen_dir_nums_15d           
  ,sum(t1.gen_dir_money_15d          ) as gen_dir_money_15d          
  ,sum(t1.pay_dir_nums_15d           ) as pay_dir_nums_15d           
  ,sum(t1.pay_dir_money_15d          ) as pay_dir_money_15d          
  ,sum(t1.gen_rel_nums_15d           ) as gen_rel_nums_15d           
  ,sum(t1.gen_rel_money_15d          ) as gen_rel_money_15d          
  ,sum(t1.pay_rel_nums_15d           ) as pay_rel_nums_15d           
  ,sum(t1.pay_rel_money_15d          ) as pay_rel_money_15d          
  ,sum(t1.gen_pla_nums_15d           ) as gen_pla_nums_15d           
  ,sum(t1.gen_pla_money_15d          ) as gen_pla_money_15d          
  ,sum(t1.pay_pla_nums_15d           ) as pay_pla_nums_15d           
  ,sum(t1.pay_pla_money_15d          ) as pay_pla_money_15d  
  ,concat(substr(t2.dim_week_id, 1, 4), '-', substr(t2.dim_week_id, 5, 2)) as dt
from
(select * from app.app_pop_manger_ad_detail_v2_day where dt >= '""" + ht.calendar.getWeekFirst() + """') t1
left outer join app.app_pop_manger_dim_day t2 on(date_format(t1.op_time, 'yyyy-MM-dd', 'yyyyMMdd') = t2.dim_day_id)
group by 
   concat(substr(t2.dim_week_id, 1, 4), '-', substr(t2.dim_week_id, 5, 2)) 
  ,t1.site_id                    
  ,t1.adspec_id                  
  ,t1.adpos_id                   
  ,t1.jd_pin                     
  ,t1.ad_id                      
  ,t1.advertiser_id              
  ,t1.item_sku_id                
  ,t1.mode_type       
;
      
--插入分月事实表
insert overwrite table app_pop_manger_ad_detail_v2_time partition(tp = 'month', dt)
select
   substr(op_time, 1, 7)            as op_time    
  ,site_id                          as site_id      
  ,adspec_id                        as adspec_id    
  ,adpos_id                         as adpos_id     
  ,jd_pin                           as jd_pin       
  ,ad_id                            as ad_id        
  ,advertiser_id                    as advertiser_id
  ,item_sku_id                      as item_sku_id  
  ,mode_type                        as mode_type    
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
  ,substr(op_time, 1, 7) as dt
from app.app_pop_manger_ad_detail_v2_day where dt >= '""" + ht.calendar.getMonthFirst() + """'
group by 
   substr(op_time, 1, 7)       
  ,site_id                         
  ,adspec_id                       
  ,adpos_id                        
  ,jd_pin                          
  ,ad_id                           
  ,advertiser_id                   
  ,item_sku_id                     
  ,mode_type        
;
"""

ht.exec_sql(schema_name = 'app', table_name = 'app_pop_manger_ad_detail_v2_time', sql = sql) 

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