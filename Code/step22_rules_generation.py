#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/3/4 20:31
# @Author  : Feng Zhanpeng
# @File    : step4_rules_generation.py
# @Software: PyCharm

"""
单维度策略泛化代码执行顺序：
1. 加载策略泛化过程中需要使用的功能函数和自动化输出函数，对应的代码见 step21_generation_fun.py；
2. 加载Python包
3. 配置相关分析文档路径
4. 读入数据并进行数据预处理；
5. 基于加载的函数进行策略自动泛化；
6. 基于步骤3策略泛化结果筛选泛化效果好的规则集进行合并泛化
"""

# 2. 加载python包
import pandas as pd
import numpy as np
import datetime
import os

# 3. 配置相关分析文档路径

# 数据存储路径，在实操时要换成自己本地的路径，path跟策略测算时候的path一模一样，粘贴过来即可
path='F:\\DataAna\\策略\\Chapter9\\9.3.1基于模型评分的单维度策略开发\\'
# 测算结果路径
path_result=path+'\\rule_result\\'
# 泛化结果输出路径，可自定义输出路径
path_rule=path_result+"20221015\\"
if not os.path.exists(path_rule):
    os.makedirs(path_rule)

# 4.读入数据并进行数据预处理

#  读入测算环节筛选的待泛化规则
rules_dict=pd.read_excel(path_result+'规则字典.xlsx')
if  'Unnamed: 0' in rules_dict.columns.tolist():
    del rules_dict['Unnamed: 0']

# 加载数据
f=open(path+'model_score_data.csv',encoding='utf-8')
mydata = pd.read_csv(f)
mydata.columns=mydata.columns.map(lambda x:x.lower())

mydata['apply_mth']=mydata.apply_mth.map(lambda x : merge_mth(x,3))

for i in mydata.columns[mydata.dtypes!= 'object']:
    mydata[i][mydata[i].map(lambda x : x in [-999,-9999,-9998,-999999,-99999,-998,-997,-9997])] = np.nan

for i in mydata.columns[mydata.dtypes== 'object']:
    mydata[i]=mydata[i].map(lambda x: str(x).strip())
    mydata[i][mydata[i].map(lambda x :x in ['-999','-9999','-9998','-999999','-99999','-998','-997','-9997'])]= np.nan
    try:
        mydata[i]=mydata[i].astype('float64')
    except:
        pass

# 处理灰样本
mydata['mob3_dpd_30_act']=mydata['mob3_dpd_30_act'].map(lambda x:1 if x==1 else 0)
mydata['mob6_dpd_30_act']=mydata['mob6_dpd_30_act'].map(lambda x:1 if x==1 else 0)
mydata['mob9_dpd_30_act']=mydata['mob9_dpd_30_act'].map(lambda x:1 if x==1 else 0)
mydata['mob12_dpd_30_act']=mydata['mob12_dpd_30_act'].map(lambda x:1 if x==1 else 0)

# 5.基于加载的函数进行策略自动泛化；（需要基于阈值测算的数据字典）

''' # 泛化参数说明
target_ripe      获取目标字段对应的是否成熟标签，与测算环节参数一样
sample_type_col  若测算样本是整个样本的子集，需配置sample_type_col参数，指名样本类型从哪个字段获取，与测算环节参数一样
rules_dict       测算环节筛选的待泛化规则集
rule_all         当前已经在线上运行的所有规则对应的决策结果标签字段，1代表拒绝，0代表未拒绝
use_credit_flag  授信申请通过后是否用信标签字段，1代表用信，0代表未用信
circle_mth       申请月对应的字段，在进行策略泛化的时候的会按月进行泛化，分析策略触碰情况和风险表现情况
circle_week      申请周对应的字段，在进行策略泛化的时候会按申请周进行泛化，分析近10周策略触碰情况
circle_day       申请日对应的字段，在进行策略泛化的时候会按申请日进行泛化，分析近10日策略触碰情况
base_lift        策略泛化时，取额外触碰样本的Lift值与base_lift进行比较，若额外触碰Lift值大于base_lift则说明策略效果较好
subset_total     是否获取全量样本作为计算整体，若取值为False，则分母为全样本，计算各种指标时是从全量样本维度考虑的，若取值为True，则分母为样本类型对应的样本
'''

target_ripe = {'mob3_dpd_30_act':['agr_mob3_dpd_30'],'mob6_dpd_30_act':['agr_mob6_dpd_30'],
               'mob9_dpd_30_act':['agr_mob9_dpd_30'],'mob12_dpd_30_act':['agr_mob12_dpd_30']}

# sample_type_col = {
#                    '类型1': ['ordertype', ['类型1']],
#                    '类型2': ['ordertype', ['类型2']]
#                   }

# 对测算环节筛选的样本进行自动化泛化
starttime = datetime.datetime.now()
print('程序开始执行时间为: ' + str(starttime))

rule_combine_results(data=mydata,rules_dict=rules_dict,rule_all='apply_refuse_flag',use_credit_flag='if_loan_flag',circle_mth='apply_mth',circle_week='apply_week',circle_day='apply_day',base_lift=3,subset_total=True)

endtime = datetime.datetime.now()
print('程序开始执行时间为: ' + str(endtime-starttime))


# 此次只分析了一个模型分变量，多个变量合并泛化的步骤不需要执行了

# 6.基于步骤3策略泛化结果筛选泛化效果好的规则集进行合并泛化（不需要测算结果数据字典）
# mydata['all_need_online_rule']=mydata.apply(lambda x: 1 if (x['var8'] <=668 or  x['var16']>17 )  else 0,axis=1 )
# starttime = datetime.datetime.now()
# print('程序开始执行时间为: ' + str(starttime))
# rule_combine_results_01(data=mydata, rule_all='apply_refuse_flag',  use_credit_flag='if_loan_flag',cut_point=0,direction='>',rule_name='待上线规则合并', rule_name_chinese='待上线规则合并',var='all_need_online_rule',rule_type='CR', rule_limit='Total', circle_mth='apply_mth',circle_week='apply_week',circle_day='apply_day',target='mob3_dpd_30_act',base_lift=3,subset_total=True)
# endtime = datetime.datetime.now()
# print('程序执行时间为: ' + str(endtime-starttime))

