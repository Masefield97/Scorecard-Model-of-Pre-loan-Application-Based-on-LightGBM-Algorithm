#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/3/4 20:28
# @Author  : Feng Zhanpeng
# @File    : step13_rule_calculate.py
# @Software: PyCharm

"""
单维度策略测算代码执行顺序：
1. 加载策略测算过程中需要使用的功能函数，对应的代码见附件中的step11_calculate_fun.py；
2. 加载基于xlsxwriter包的分析结果自动化输出函数，对应的代码见附件中的step12_output_fun.py；
3. 加载Pyhon包
4. 配置相关分析文档路径及分析结果输出文档名称
5. 读入数据并进行数据预处理；
6. 配置函数所需的各种参数；
7. 基于加载的函数进行描述性统计分析、变量分箱、规则效果分析和筛选、变量分析和筛选结果汇总；
8. 基于xlsxwriter包自动化输出策略测算结果；
9. 基于xlsxwriter包自动化输出待泛化的策略；
"""

# 3. 加载Python包
import pandas as pd
import numpy as np
import datetime
import re
import os
import xlsxwriter

# 4. 配置相关分析文档路径及分析结果输出文档名称

# 数据存储路径，在实操时要换成自己本地的路径
path='F:\\DataAna\\策略\\Chapter9\\9.3.1基于模型评分的单维度策略开发\\'
# 分析结果输出路径
path_result=path+'\\rule_result\\'
if not os.path.exists(path_result):
    os.makedirs(path_result)

#  加载策略测算生成的Excel文件名
starttime = datetime.datetime.now()
print('程序开始运行时间为:'+ str(starttime))
excel_name='Var_ana_result_'+starttime.strftime('%Y%m%d%H')+'.xlsx'

# 5.读入数据并进行数据预处理

# 读取变量数据字典
var_dict = pd.read_excel(path + "待分析变量的数据字典.xlsx")
var_dict.变量名 = var_dict.变量名.map(lambda x: str(x).lower().replace('\t', ''))

# 加载数据,测算样本区间为202003-202103
f=open(path+'model_score_data.csv',encoding='utf-8')
my_data=pd.read_csv(f)
my_data=my_data[(my_data['apply_mth'].map(lambda x : (x>='2020-03' and x<='2021-09' )))& (my_data['if_loan_in_30']==1)]
my_data.columns=[x.lower() for x in my_data.columns]

# 删除不需要分析的字段
my_data=my_data[['score_int','agr_mob3_dpd_30','agr_mob6_dpd_30','agr_mob9_dpd_30','mob3_dpd_30_act',
                 'mob6_dpd_30_act','mob9_dpd_30_act','mob12_dpd_30_act','agr_mob12_dpd_30']]

# 处理缺失值：变量中取值为-999，-9999，-999999代表该取值缺失
for i in my_data.columns[my_data.dtypes!= 'object']:
    my_data[i][my_data[i].map(lambda x : x in (-999,-9999,-9998,-999999,-99999,-998,-997,-9997))] = np.nan

# 处理分类型变量
for i in my_data.columns[my_data.dtypes== 'object']:
    my_data[i]=my_data[i].map(lambda x: str(x).strip())
    my_data[i][my_data[i].map(lambda x :x in ['-999','-9999','-9998','-999999','-99999','-998','-997','-9997'])]= np.nan
    try:
        my_data[i]=my_data[i].astype('float64')
    except:
        del my_data[i]

# 处理灰样本
my_data['mob3_dpd_30_act']=my_data['mob3_dpd_30_act'].map(lambda x:1 if x==1 else 0)
my_data['mob6_dpd_30_act']=my_data['mob6_dpd_30_act'].map(lambda x:1 if x==1 else 0)
my_data['mob9_dpd_30_act']=my_data['mob9_dpd_30_act'].map(lambda x:1 if x==1 else 0)
my_data['mob12_dpd_30_act']=my_data['mob12_dpd_30_act'].map(lambda x:1 if x==1 else 0)

my_data1=my_data

'''
6.配置函数所需的各种参数
seq ：变量计数开始序号，若有10个变量，计算结果为seq,seq+1,...,seq+9；
sample_type ： 测算样本类型，本节中的代码支持同时测算多个不同类型的样本，如可同时对不同产品样本进行策略挖掘；
sample_type_col ： 若测算样本是全量样本（Total），则不需要配置该参数，否则需配置该参数，指名样本类型从哪个字段获取如何获取；
sample_type_target ： 测算不同样本类型时对应要分析的目标字段；
target_ripe ： 获取目标字段对应的是否成熟标签；
target_del_col ： 筛选完样本后，最终应只剩下待测算变量和目标字段，目标字段在最后一列。该参数表示删除不需要用到的字段；
sub_div_bin ：头部和尾部需要精细化分析的样本占比（比如5%，表示首尾5%的样本需要精细化分箱分析）；
min_num ： 每箱最小样本数；
target_min_rate ： 不同目标字段对应的分箱中，每箱最小占比；
sample_type_lift ： 规则阈值确定后，筛选的样本要满足的最小lift，不同目标字段，可设置不同的lift，基于lift衡量规则效果；
hit_num ： 虽然设置了min_num参数，但是因为数据分布不均匀，筛选出来的分箱样本量可能会小于min_num。虽然最终的分箱满足其他预设的各种条件，如Lift表现较好，但是若样本量太少不满足大数定律也是不建议作为规则使用的，hit_num参数就是在min_num参数的基础上，对筛选规则触碰样本进行强制限制，小于该值不筛选该规则
'''

# 参数值设置
sample_range = '202003-202103'
seq = 1
# sample_type取值只有Total，表示测算全量样本，不需要配置参数sample_type_col；若还有其他样本类型，也支持同时测算
sample_type = ['Total']

# 若sample_type需测算类型1和类型2的样本，类型1和类型2所在的字段为ordertype，sample_type_col参数示例如下
# sample_type_col = {
#                    '类型1': ['ordertype', ['类型1']],
#                    '类型2': ['ordertype', ['类型2']]
#                   }

sample_type_target = {'Total': ['mob3_dpd_30_act','mob6_dpd_30_act','mob9_dpd_30_act','mob12_dpd_30_act']}

target_ripe = {'mob3_dpd_30_act':['agr_mob3_dpd_30'],'mob6_dpd_30_act':['agr_mob6_dpd_30'],
               'mob9_dpd_30_act':['agr_mob9_dpd_30'],'mob12_dpd_30_act':['agr_mob12_dpd_30']}

target_del_col = {'mob3_dpd_30_act':['agr_mob3_dpd_30','mob6_dpd_30_act','agr_mob6_dpd_30','mob9_dpd_30_act','agr_mob9_dpd_30','mob12_dpd_30_act','agr_mob12_dpd_30'],
                  'mob6_dpd_30_act':['agr_mob6_dpd_30','mob3_dpd_30_act','agr_mob3_dpd_30','mob9_dpd_30_act','agr_mob9_dpd_30','mob12_dpd_30_act','agr_mob12_dpd_30'],
                  'mob9_dpd_30_act':['agr_mob9_dpd_30','mob3_dpd_30_act','agr_mob3_dpd_30','mob6_dpd_30_act','agr_mob6_dpd_30','mob12_dpd_30_act','agr_mob12_dpd_30'],
                  'mob12_dpd_30_act':['agr_mob12_dpd_30','mob3_dpd_30_act','agr_mob3_dpd_30','mob6_dpd_30_act','agr_mob6_dpd_30','mob9_dpd_30_act','agr_mob9_dpd_30']
                  }

sub_div_bin = 0.1
target_min_rate =  {'mob3_dpd_30_act':[0.01],'mob6_dpd_30_act':[0.01],'mob9_dpd_30_act':[0.01],'mob12_dpd_30_act':[0.01]}
min_num = 40
hit_num=30
sample_type_lift = { 'Total': {'mob3_dpd_30_act':2,'mob6_dpd_30_act':2,'mob9_dpd_30_act':2,'mob12_dpd_30_act':2}}

# 7.基于加载的函数进行描述性统计分析、变量分箱、规则效果分析和筛选、分析结果和待泛化策略自动化输出

# 变量描述性统计分析，ana_people表示策略分析人是谁
var_select_01=describe_stat_ana(describe_data=my_data1,sample_range=sample_range ,seq=seq,sample_type=sample_type,ana_people='fzp')

# 策略测算效果分析和筛选
filter2=bin_result_summary_final(hit_num=hit_num,bindata=my_data1,var_select01=var_select_01,sub_div_bin=sub_div_bin,min_num=min_num,sample_type=sample_type,method='best',numOfSplit=10)

# 获取变量分箱结果明细
bins_result_detail=bin_result_detail(bindata=my_data1,var_select02=filter2,sample_type=sample_type,sub_div_bin=sub_div_bin,min_num=min_num,method='best',numOfSplit=10)

# 变量分析和筛选情况
summary_info=get_summary(filter2)

# 8.基于xlsxwriter包自动化输出策略测算结果

wb = xlsxwriter.Workbook(path_result+excel_name)
var_summary_result_output(wb=wb,sheetname='变量筛选汇总',data=summary_info,start=0)
summary_result_output(wb=wb,sheetname='1.变量基础分析和筛选',data=var_select_01)
details_result_output(wb=wb,sheetname='2.变量分箱',data=bins_result_detail, suoyin=0, ana_people='fzp')
# 此处因分析的变量较少，未对筛选出来的规则使用的变量进行相关性分析和筛选
summary_result_output(wb=wb,sheetname='3.变量效果分析和筛选',data=filter2)
wb.close()

# 9.基于xlsxwriter包自动化输出待泛化的策略

var_summary = filter2
# 筛选标签3为Y的规则进行泛化
var_summary01=var_summary[var_summary.标签3=='Y']
var_summary02=var_summary01[['序号','分析时间','样本类型','样本区间','坏客户定义', '变量英文名', '变量中文名','type','Threshold','%Bad_Rate(包含缺失值)', '#Obs','%Obs', '#Bad', '%Bin_Bad_Rate','Odds','Lift']]

var_summary02.rename(columns={'序号':'Seq','分析时间':'Ana_Date','样本类型':'Rule_Limit','样本区间':'Sample_Range','坏客户定义':'Target', '变量英文名':'Var', '变量中文名':'Description','type':'Direction','%Bad_Rate(包含缺失值)':'%Bad_Rate'},inplace=True)

varChinese=[]
for i,j in zip(var_summary02.loc[:,'Var'],var_summary02.loc[:,'Description']):
    print(i,j)
    try:
        pipei = re.findall('[0-9]+.{1,3}', i)[-1:][0]
        suoyin = re.search(pipei, i).start()
        pipei = i[suoyin:]
        varChinese.append(j.replace('XX时间',pipei))
    except:
        varChinese.append(j)

var_summary02['Description']=varChinese

var_summary02['Rule_Name']=['single_var_'+ i for i in var_summary02.loc[:,'Seq'] ]
var_summary02['Rule_Category']='单变量规则'
var_summary02['Rule_Type']=var_summary02['Target'].map(lambda x:'FR' if x=='fpd_30_act' else 'CR')

var_summary02=var_summary02[['Ana_Date','Seq','Sample_Range','Rule_Name','Rule_Category','Rule_Type','Rule_Limit','Target','Var','Description','Direction','Threshold','%Bad_Rate', '#Obs', '%Obs','#Bad', '%Bin_Bad_Rate', 'Odds','Lift']]

var_summary02['id']=var_summary02.Seq.map(lambda x: int(x.replace('fzp','')))
var_summary02.sort_values(by='id',inplace=True)
del var_summary02['id']

# 待泛化规则自动化输出
wb = xlsxwriter.Workbook(path_result+'规则字典.xlsx')
var_summary_result_output_01(wb=wb,sheetname='规则字典',data=var_summary02)
wb.close()

