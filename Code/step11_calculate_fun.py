#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/3/4 19:53
# @Author  : Feng Zhanpeng
# @File    : step11_calculate_fun.py
# @Software: PyCharm

"""
单维度策略测算部分指标计算相关函数，直接加载即可
"""

import re

'''
1. 变量描述性统计分析函数
'''
def describe_stat_ana(describe_data,sample_range,seq,sample_type,ana_people):
    """
    :param describe_data:      需要分析的数据框
    :param sample_range:       样本区间
    :param seq:                分析变量从seq开始计数
    :param sample_type:        分析的样本类型，列表格式
    :param ana_people:         分析人
    :return:                   变量的描述性统计分析结果
    """
    col_seq = ["序号", "分析时间", "样本类型", "坏客户定义", "变量英文名", "变量中文名", "样本区间","%Bad_Rate(不含缺失值)", "%Bad_Rate(包含缺失值)",
               "总样本量","坏样本量", "缺失量", "缺失率","变量取值数（包含缺失值）", "变量取值数（不含缺失值）", "单一值最大占比的变量值", "单一值最大占比的样本量",
               "单一值最大占比", "单一值第二大占比的变量值", "单一值第二大占比的样本量","单一值第二大占比", "单一值第三大占比的变量值", "单一值第三大占比的样本量",
               "单一值第三大占比","单一值前二大占比的总样本量", "单一值前二大占比总和", "单一值前三大占比的总样本量", "单一值前三大占比总和",
                "最大值", "最大值数量", "最大值占比", "最小值", "最小值数量", "最小值占比","平均值", "下四分位数", "中位数", "上四分位数",
               "标准差", "离散系数", "标签1"]
    var_detail = pd.DataFrame(columns=col_seq)
    for sample_type_sub in sample_type:
        print('正在分析的样本类型为：', sample_type_sub)
        if '其他' in sample_type_sub:
            describedata = describe_data[describe_data[sample_type_col[sample_type_sub][0]].map(
                lambda x: str(x) not in sample_type_col[sample_type_sub][1])]
        elif 'Total' in sample_type_sub:
            describedata = describe_data
        else:
            describedata = describe_data[describe_data[sample_type_col[sample_type_sub][0]].map(
                lambda x: str(x) in sample_type_col[sample_type_sub][1])]
        target = sample_type_target[sample_type_sub]
        for target_sub in target:
            print("分析的目标字段为：", target_sub)
            mydata1 = describedata[describedata[target_ripe[target_sub][0]]==1]   ##获取成熟样本
            mydata1 = mydata1.drop(labels=target_del_col[target_sub], axis=1)
            for var in mydata1.columns[:-1]:
                print('正在分析的变量为:', var)
                seq1 = ana_people + str(seq)
                ana_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
                sample_des = sample_type_sub
                var_english_name = var
                var_chinese_name = var_dict.变量描述[var_dict.变量名 == var].values[0] if sum(var_dict.变量名 == var) else var
                sample_range = sample_range
                bad_define = target_sub
                # 计算badrate,包含和不包含缺失值
                data_nona = mydata1[[var, target_sub]].dropna()
                data_withna = mydata1[[var, target_sub]]
                bad_rate_nona = data_nona[target_sub].value_counts(normalize=True)[1] if \
                    len(data_nona[target_sub].value_counts(normalize=True)) > 1 else 0
                bad_rate_withna = data_withna[target_sub].value_counts(normalize=True)[1]
                total_cnt = len(data_withna)
                bad_cnt =sum(mydata1[target_sub]==1)
                na_cnt = sum(mydata1[var].isnull())
                na_rate = na_cnt * 1.0 / total_cnt
                unique_cnt_withna = len(data_withna[var].unique())
                unique_cnt_nona = len(data_nona[var].unique())
                ####前3大占比值分析
                first_info_rate = data_withna[var].value_counts(dropna=False, normalize=True).sort_values(
                    ascending=False)
                first_info_cnt = data_withna[var].value_counts(dropna=False).sort_values(ascending=False)
                max_cnt_value = first_info_rate.index[0]
                max_cnt_value_num = first_info_cnt.tolist()[0]
                max_cnt_value_rate = first_info_rate.max()
                second_cnt_value = first_info_rate.index[1] if len(first_info_rate) > 1 else np.nan
                second_cnt_value_num = first_info_cnt.tolist()[1] if len(first_info_cnt) > 1 else np.nan
                second_cnt_value_rate = first_info_rate.tolist()[1] if len(first_info_rate) > 1 else np.nan
                second_cnt_value_rate_01 = first_info_rate.tolist()[1] if len(first_info_rate) > 1 else np.nan
                third_cnt_value = first_info_rate.index[2] if len(first_info_rate) > 2 else np.nan
                third_cnt_value_num = first_info_cnt.tolist()[2] if len(first_info_cnt) > 2 else np.nan
                third_cnt_value_rate = first_info_rate.tolist()[2] if len(first_info_rate) > 2 else np.nan
                third_cnt_value_rate_01 = first_info_rate.tolist()[2] if len(first_info_rate) > 2 else np.nan
                var_max_cnt_value_12num = np.nansum([max_cnt_value_num, second_cnt_value_num])
                var_max_cnt_value_12rate = np.nansum([first_info_rate.tolist()[0], second_cnt_value_rate_01])
                var_max_cnt_value_123num = np.nansum([max_cnt_value_num, second_cnt_value_num, third_cnt_value_num])
                var_max_cnt_value_123rate = np.nansum(
                    [first_info_rate.tolist()[0], second_cnt_value_rate_01, third_cnt_value_rate_01])
                max_value = data_nona[var].max()
                max_value_num = sum(data_nona[var] == max_value)
                max_value_rate = sum(data_nona[var] == max_value) / total_cnt
                min_value = data_nona[var].min()
                min_value_num = sum(data_nona[var] == min_value)
                min_value_rate = sum(data_nona[var] == min_value) / total_cnt
                mean_value = data_nona[var].mean()
                q1_value = np.percentile(data_nona[var], 25) if len(data_nona) > 0 else np.nan
                median_value = data_nona[var].median()
                q3_value = np.percentile(data_nona[var], 75) if len(data_nona) > 0 else np.nan
                std_value = np.std(data_nona[var])
                cv = std_value / mean_value if mean_value != 0 else 0
                top_1_2_rate = np.nansum([first_info_rate.tolist()[0], second_cnt_value_rate_01])
                if (top_1_2_rate < 0.999) & (unique_cnt_withna >= 2):
                    if_trigger_choose = 'Y'
                else:
                    if_trigger_choose = 'N'
                sum_info = [seq1, ana_time, sample_des, bad_define, var_english_name, var_chinese_name, sample_range,
                            bad_rate_nona, bad_rate_withna, total_cnt, bad_cnt, na_cnt, na_rate, unique_cnt_withna,
                           unique_cnt_nona,max_cnt_value, max_cnt_value_num,max_cnt_value_rate, second_cnt_value,
                            second_cnt_value_num, second_cnt_value_rate,third_cnt_value, third_cnt_value_num,
                            third_cnt_value_rate, var_max_cnt_value_12num,var_max_cnt_value_12rate,var_max_cnt_value_123num,
                            var_max_cnt_value_123rate, max_value, max_value_num, max_value_rate, min_value,
                           min_value_num, min_value_rate, mean_value, q1_value, median_value, q3_value,
                           std_value, cv, if_trigger_choose]
                sum_info01 = pd.DataFrame(sum_info).T
                sum_info01.columns = col_seq
                var_detail = var_detail.append(sum_info01, ignore_index=True)
                seq += 1
    return (var_detail)


'''
2. 变量分箱函数
'''
# 2.1 变量分箱函数
def get_bin_lift(data,flag_name,factor_name,min_rate=0.003,sub_div_bin=0.05,min_num=30,method='best',numOfSplit=10):
    """
    :param data:            需要分析的数据框
    :param flag_name:       要分析的目标字段
    :param factor_name:     要分析的变量
    :param min_rate:        最小分箱占比
    :param sub_div_bin:     头部和尾部分箱占比
    :param min_num:         最小分箱样本量
    :param method:          分箱方法：最优分箱、等频分箱
    :param numOfSplit:      等频分箱对应的分箱数量
    :return:                分箱结果
    """
    k=group_by_var_value(data,flag_name,factor_name,'#Bad','#Good')
    k1 = get_na_bin(data, flag_name, factor_name, '#Bad', '#Good')
    if len(k)==0:
        print(flag_name,factor_name,' have no value')
        return pd.DataFrame()
    if method == 'best':
        total = len(data[data[factor_name].notnull()])
        min_rate_act=max(min_num/total,min_rate)
        knot_start=[]
        obs_start=[]
        knot_end = []
        obs_end=[]
        if sub_div_bin>=min_rate_act:
            end_cnt=int(sub_div_bin/min_rate_act)
        else:
            end_cnt=int(min_rate_act/sub_div_bin)
        for i in range(end_cnt):
            if len(knot_start) == 0:
                tmp=k[k['%Cum_Obs']>=min_rate_act].index.tolist()
            else:
                tmp=k[k['%Cum_Obs']>=min_rate_act+obs_start[-1]].index.tolist()
            if len(tmp) > 0:
                knot_start.append(tmp[0])
                obs_start.append(k['%Cum_Obs'][tmp[0]])
        for i in range(end_cnt):
            if len(knot_end) == 0:
                tmp=k[k['%Opps_Cum_Obs']>=min_rate_act].index.tolist()
            else:
                tmp=k[k['%Opps_Cum_Obs']>=min_rate_act+obs_end[-1]].index.tolist()
            if len(tmp) > 0:
                if tmp[-1]>0:
                    knot_end.append(tmp[-1]-1)
                    obs_end.append(k['%Opps_Cum_Obs'][tmp[-1]-1])
        knot = sorted(list(set(knot_start + knot_end)))
        if len(k) - 1 in knot:
            knot.remove(len(k) - 1)
        if len(knot) > 15:
            knot = knot[0:7] + knot[-7:]
    elif method == 'equalfreq':
        knot_value = unsupervise_splitbin(data,factor_name,numOfSplit,method)
        knot=[k[k[factor_name]==i].index[0] for i in knot_value ]
    res1 = important_bin_calculate(k, k1, '#Good', '#Bad', factor_name, [0] + knot + [len(k) - 1])
    return res1


# 计算变量每一种取值、每一种指标取值下好样本的个数、坏样本的个数
def group_by_var_value(data, flag_name, factor_name, bad_name, good_name, discrete_list=[]):
    """
    :param data:            需要分析的数据框
    :param flag_name:       要分析的目标字段
    :param factor_name:     要分析的变量
    :param bad_name:        坏样本个数列名
    :param good_name:       好样本个数列名
    :param discrete_list:   分类变量列表
    :return:                变量每一种取值、每一种指标取值下好样本的个数、坏样本的个数
    """
    if len(data) == 0:
        return pd.DataFrame()
    regroup1 = data.groupby([factor_name])[flag_name].count()
    regroup2 = data.groupby([factor_name])[flag_name].sum()
    data1 = pd.DataFrame({good_name: regroup1 - regroup2, bad_name: regroup2}).reset_index()
    good = float(sum(data1[good_name]))
    bad = float(sum(data1[bad_name]))
    total = good + bad
    data1['%Bad_Rate'] = data1[bad_name] / (data1[bad_name] + data1[good_name])
    data1['#Obs'] = (data1[good_name] + data1[bad_name])
    data1['%Obs'] = (data1[good_name] + data1[bad_name]) / total
    data1['%Cum_Obs'] = np.cumsum(data1['%Obs'])
    data1['%Opps_Cum_Obs'] = (1 - np.cumsum(data1['%Obs'])) + data1['%Obs']
    if factor_name not in discrete_list:
        data1 = data1.sort_values(by=[factor_name], ascending=True)
        data1['Char_Type'] = 'numeric'
    else:
        data1 = data1.sort_values(by=['%Bad_Rate'], ascending=True)
        data1['Char_Type'] = 'non-numeric'
    data1 = data1.reset_index(drop=True)
    return data1


def get_str(x):
    """
    将数字转为字符串
    """
    if type(x) in [float, np.float64, np.float16, np.float32]:
        return ('{0:.17}'.format(x))
    elif type(x) in [int, np.int8, np.int16, np.int32, np.int64]:
        return str(x)
    else:
        try:
            return str(x)
        except:
            return x

# 根据切分点获得指标的分组结果
def important_bin_calculate(data_df,na_df, good_name, bad_name, factor_name, knots_list,if_sort=False):
    """
    :param data_df:         转换后的数据框
    :param na_df:           要分析的目标字段
    :param good_name:       好样本个数列名
    :param bad_name:        坏样本个数列名
    :param factor_name:     指标列名
    :param knots_list:      最佳分组点集合
    :param if_sort:         是否需要对指标的分组结果进行排序
    :return:                根据切分点获得指标的分组结果
    """
    flag = data_df['Char_Type'].max()
    temp_df_list = []
    bin_list = []
    for i in range(1, len(knots_list)):
        if i == 1:
            temp_df_list.append(data_df.loc[knots_list[i - 1]:knots_list[i]])
            if flag == 'numeric':
                bin_list.append('(-inf, ' + get_str(data_df[factor_name][knots_list[i]]) + ']')
            else:
                bin_list.append(list(data_df[factor_name])[knots_list[i - 1]:knots_list[i] + 1])
        else:
            temp_df_list.append(data_df.loc[knots_list[i - 1] + 1:knots_list[i]])
            if flag == 'numeric':
                if knots_list[i - 1] + 1 == knots_list[i]:
                    bin_list.append('[' + get_str(data_df[factor_name][knots_list[i]]) + ']')
                elif i == len(knots_list) - 1:
                    bin_list.append('(' + get_str(data_df[factor_name][knots_list[i - 1]]) + ', inf)')
                else:
                    bin_list.append(
                        '(' + get_str(
                            data_df[factor_name][knots_list[i - 1]]) + ', ' + get_str(
                            data_df[factor_name][knots_list[i]]) + ']')
            else:
                bin_list.append(list(data_df[factor_name])[knots_list[i - 1] + 1:knots_list[i] + 1])
    if len(knots_list) == 2:
        bin_list = ['(-inf, inf)']
    if len(na_df) != 0:
        na_good = sum(na_df[good_name])
        na_bad = sum(na_df[bad_name])
        total_good = sum(data_df[good_name]) + na_good
        total_bad = sum(data_df[bad_name]) + na_bad
        temp_df_list.append(na_df)
        bin_list.append("缺失值")
    else:
        na_good = 0
        na_bad = 0
        total_good = sum(data_df[good_name])
        total_bad = sum(data_df[bad_name])
    good_list = list(map(lambda x: sum(x[good_name]), temp_df_list))
    bad_list = list(map(lambda x: sum(x[bad_name]), temp_df_list))
    good_percent_series = pd.Series(list(map(lambda x: float(sum(x[good_name])) / total_good, temp_df_list)))
    bad_percent_series = pd.Series(list(map(lambda x: float(sum(x[bad_name])) / total_bad, temp_df_list)))
    woe_list = list(np.log(good_percent_series / bad_percent_series))
    IV_list = list((good_percent_series - bad_percent_series) * np.log(good_percent_series / bad_percent_series))
    total_list = list(map(lambda x: sum(x[good_name]) + sum(x[bad_name]), temp_df_list))
    bin_rate_list = list(
        map(lambda x: float(sum(x[good_name]) + sum(x[bad_name])) / (total_good + total_bad), temp_df_list))
    non_na_indicator = pd.DataFrame({'Bin': bin_list,
                                         '#Obs': total_list,
                                         '#Good': good_list,
                                         '#Bad': bad_list,
                                         'IV(bin)': IV_list,
                                         'WOE': woe_list,
                                         '%Obs': bin_rate_list})
    l = ['Bin', '#Obs', '%Obs', '#Cum_Obs', '%Cum_Obs', '#Good', '%Good', '#Cum_Good', '%Cum_Good',
         '#Bad', '%Bad', '#Cum_Bad', '%Cum_Bad', '%Bad_Rate', 'WOE', 'IV(bin)', 'IV(total)', 'Odds1', 'Odds2',
         'Lift']
    result_indicator = non_na_indicator.reset_index(drop=True)
    result_indicator = result_indicator[result_indicator['Bin'] != 'NA']
    if if_sort:
        result_indicator = result_indicator.sort_index(ascending=False).reset_index()
    result_indicator['%Cumulative_Bad_Rate'] = np.cumsum(result_indicator['#Bad']) / np.cumsum(
        result_indicator['#Obs'])
    result_indicator['WOE'] = result_indicator['WOE'].map(lambda x: 0 if x in [np.inf, -np.inf] else x)
    result_indicator['IV(bin)'] = result_indicator['IV(bin)'].map(lambda x: 0 if x in [np.inf, -np.inf] else x)
    result_indicator['#Cum_Obs'] = np.cumsum(result_indicator['#Obs'])
    result_indicator['%Cum_Obs'] = np.cumsum(result_indicator['%Obs'])
    result_indicator['%Good'] = result_indicator['#Good'] / sum(result_indicator['#Good'])
    result_indicator['#Cum_Good'] = np.cumsum(result_indicator['#Good'])
    result_indicator['%Cum_Good'] = result_indicator['#Cum_Good'] / sum(result_indicator['#Good'])
    result_indicator['%Bad'] = result_indicator['#Bad'] / sum(result_indicator['#Bad'])
    result_indicator['#Cum_Bad'] = np.cumsum(result_indicator['#Bad'])
    result_indicator['%Cum_Bad'] = result_indicator['#Cum_Bad'] / sum(result_indicator['#Bad'])
    result_indicator['%Bad_Rate'] = result_indicator['#Bad'] / result_indicator['#Obs']
    result_indicator['IV(total)'] = np.cumsum(result_indicator['IV(bin)'])
    if len(na_df) != 0:
        result_indicator_1=result_indicator[result_indicator['Bin']!='缺失值']
        result_indicator['Odds1']=((result_indicator_1['#Cum_Bad'] / result_indicator_1['#Cum_Good']) / (
        (total_bad -na_bad- result_indicator_1['#Cum_Bad']) / (total_good -na_good- result_indicator_1['#Cum_Good']))).tolist()+[np.nan]
        result_indicator['Odds2']=[np.nan] +list(1/result_indicator['Odds1'])[0:-2]+[np.nan]
    else:
        result_indicator['Odds1'] = (result_indicator['#Cum_Bad'] / result_indicator['#Cum_Good']) / (
            (total_bad - result_indicator['#Cum_Bad']) / (total_good - result_indicator['#Cum_Good']))
        result_indicator['Odds2'] = [np.nan] + list(1 / result_indicator['Odds1'])[0:-1]
    result_indicator['Lift'] = result_indicator['%Bad_Rate'] / (sum(result_indicator['#Bad']) / (sum(result_indicator['#Obs'])))
    result_indicator = result_indicator.replace(np.inf, 0)
    return result_indicator[l]

# 获取缺失值分箱
def get_na_bin(data_total, flag_name, factor_name, good_name, bad_name):
    """
    :param data_total:         全量数据框
    :param flag_name:          要分析的目标字段
    :param factor_name:        要分析的变量列名
    :param good_name:          好样本个数列名
    :param bad_name:           坏样本个数指标列名
    :return:                   缺失值分箱
    """
    data = data_total[(data_total[factor_name].isnull())]
    good_cnt = data[flag_name].sum()
    tn = len(data[data[flag_name].notnull()])
    na_df = pd.DataFrame([["缺失值", good_cnt, tn - good_cnt]],columns=[factor_name, good_name, bad_name])
    return na_df

# 无监督分箱
def unsupervise_splitbin(df,var,numOfSplit, method = 'equalfreq'):
    '''
    :param df:            要分箱数据框
    :param var:           需要分箱的变量。仅限数值型。
    :param numOfSplit:    需要分箱个数
    :param method:        分箱方法，'equal freq'：等频，否则是等距
    :return:              分箱索引或分箱临界点
    '''
    df=df[~df[var].isnull()]
    if method == 'equalfreq':
        N = df.shape[0]
        n = np.int(N / numOfSplit)
        splitPointIndex = [i * n for i in range(1, numOfSplit)]
        rawValues = sorted(list(df[var]))
        splitPoint = [rawValues[i] for i in splitPointIndex]
        splitPoint = sorted(list(set(splitPoint)))
        return splitPoint
    if method =='equallen':
        var_max, var_min = max(df[var]), min(df[var])
        interval_len = (var_max - var_min)*1.0/numOfSplit
        splitPoint = [var_min + i*interval_len for i in range(1,numOfSplit)]
        return splitPoint

'''
3. 变量效果分析和筛选
'''
# 变量最佳切分点确定&变量效果分析和筛选  根据指标分组结果筛选出最优的odds和对应阈值
def select_best_lift(data,flag_name,factor_name,min_rate=0.003,sub_div_bin=0.05,min_num=30,hit_num=30,
                     min_lift=1.5,method='best',numOfSplit=10):
    """
    :param data:                要分析的数据框
    :param flag_name:           要分析的目标字段
    :param factor_name:         指标列名
    :param min_rate:            头部和尾部最小分箱占比
    :param sub_div_bin:         头部和尾部分箱占比
    :param min_num:             最小分箱样本量
    :param hit_num:             最小分箱触碰样本量
    :param min_lift:            最小lift值
    :param method:              分箱方法
    :param numOfSplit:          等频分箱对应的分箱数量
    :return:                    策略阈值及对应的触碰量、风险倍数、是否筛选等指标
    """
    bin_odd = get_bin_lift(data, flag_name, factor_name, min_rate, sub_div_bin, min_num,method,numOfSplit)
    odd1 = bin_odd[bin_odd['%Cum_Obs'] < sub_div_bin]
    sp = list(bin_odd[bin_odd['Bin'] == '缺失值']['%Obs'].to_dict().values())[0]
    odd2 = bin_odd.loc[bin_odd[bin_odd['%Cum_Obs'] >= 1 - sp - sub_div_bin].index[1:]]
    dic={'var_name':factor_name}
    max1 = odd1['Odds1'].max()
    max2 = odd2['Odds2'].max()
    if (np.isnan(max1)) and (np.isnan(max2)):
        print(factor_name+' 未分箱成功或者非缺失值分箱中无坏样本，请关注')
        return dic
    elif (np.isnan(max1)) or (max1<=max2):
        max2=odd2['Odds2'].max()
        max_index=odd2[odd2['Odds2'] == max2].index[0]
        dic['Odds'] = max2
        bin_detail=bin_odd['Bin'][max_index]
        if ',' not in bin_detail:
            max_index2 = max_index-1
            bin_detail = bin_odd['Bin'][max_index2]
            print(bin_detail)
            if ',' not in bin_detail:
                dic['Threshold'] = float(bin_detail.split(' ')[-1][1:-1])
            else:
                dic['Threshold']=float(bin_detail.split(' ')[-1][0:-1])
            print(dic['Threshold'])
            if len(str(dic['Threshold']))==0:
                dic['Threshold'] = float(bin_odd['Bin'][max_index2].split(' ')[-1][1:-1])
        else:
            dic['Threshold'] = float(bin_detail.split(',')[0][1:])
        print(dic)
        dic['type'] = '>'
        bin_odd_1=bin_odd[bin_odd['Bin'] != '缺失值']
        dic['#Obs'] = bin_odd_1['#Obs'][max_index:].sum()
        dic['%Obs'] = bin_odd_1['%Obs'][max_index:].sum()
        dic['#Bad'] = bin_odd_1['#Bad'][max_index:].sum()
        dic['%Bad_Rate'] = dic['#Bad'] / dic['#Obs']
        dic['Lift'] = dic['%Bad_Rate']/(bin_odd['#Bad'].sum()/bin_odd['#Obs'].sum()) if (bin_odd['#Bad'].sum()/bin_odd['#Obs'].sum())>0 else 0
        dic['Selected'] = 'Y' if (dic['Lift'] >= min_lift and dic['#Obs']>=hit_num) else 'N'
    else:
        max1 = odd1['Odds1'].max()
        max_index = odd1[odd1['Odds1'] == max1].index[0]
        dic['Odds'] = max1
        bin_detail = bin_odd['Bin'][max_index]
        if ',' not in bin_detail:
            dic['Threshold'] = float(bin_detail.split(' ')[-1][1:-1])
        else:
            dic['Threshold'] = float(bin_detail.split(',')[1][:-1])
        dic['type'] = '<='
        dic['#Obs'] = bin_odd['#Obs'][0:(max_index + 1)].sum()
        dic['%Obs'] = bin_odd['%Obs'][0:(max_index + 1)].sum()
        dic['#Bad'] = bin_odd['#Bad'][0:(max_index + 1)].sum()
        dic['%Bad_Rate'] = dic['#Bad'] / dic['#Obs']
        dic['Lift'] = dic['%Bad_Rate']/(bin_odd['#Bad'].sum()/bin_odd['#Obs'].sum()) if (bin_odd['#Bad'].sum()/bin_odd['#Obs'].sum())>0 else 0
        dic['Selected'] = 'Y' if (dic['Lift'] >= min_lift and dic['#Obs']>=hit_num) else 'N'
    return dic


'''
4. 分析结果汇总、整合函数
'''
# 规则效果分析和统计
def bin_result_summary(bindata,bin_summary,sample_type,sub_div_bin,min_num,hit_num,method,numOfSplit):
    '''
    :param bindata:     传入需要分箱的数据
    :param bin_summary: 详见函数  bin_result_summary_final
    :param sample_type: 样本类型  可同时分析不同样本类型的数据
    :param sub_div_bin: 头部和尾部样本占比
    :param min_num:     最小分箱数量
    :param method:      分箱方法
    :param numOfSplit:  分箱数量
    :return:
    '''
    for sample_type_sub in sample_type:
        print('正在分析的样本类型为：', sample_type_sub)
        if '其他' in sample_type_sub:
            describedata = bindata[bindata[sample_type_col[sample_type_sub][0]].map(
                lambda x: str(x) not in sample_type_col[sample_type_sub][1])]
        elif 'Total' in sample_type_sub:
            describedata = bindata
        else:
            describedata = bindata[bindata[sample_type_col[sample_type_sub][0]].map(
                lambda x: str(x) in sample_type_col[sample_type_sub][1])]
        target = sample_type_target[sample_type_sub]
        for target_sub in target:
            print("分析的目标字段为：", target_sub)
            mydata1 = describedata[describedata[target_ripe[target_sub][0]] == 1]  ##获取成熟样本
            mydata1 = mydata1.drop(labels=target_del_col[target_sub], axis=1)
            min_rate=target_min_rate[target_sub][0]
            print(sample_type_sub, target_sub, '每箱最小占比：', min_rate, '数据量：', len(mydata1))
            for var in mydata1.columns[:-1]:
                print('正在分析变量:', var)
                try:
                    sample_bin = select_best_lift(data=mydata1, flag_name=target_sub, factor_name=var,
                                                  min_rate=min_rate,  sub_div_bin=sub_div_bin, min_num=min_num,hit_num=hit_num,
                                                  min_lift=sample_type_lift[sample_type_sub][target_sub],method=method,numOfSplit=numOfSplit)
                    if len(sample_bin) <= 2:
                        cnt = mydata1[var].count()
                        data1 = mydata1[[var, target_sub]].dropna()
                        bad = sum(data1[target_sub] == 1)
                        badrate = bad / cnt
                        sample_bin = {'#Bad': bad, '#Obs': cnt, '%Bad_Rate': badrate, '%Obs': 1, 'Odds': 1,
                                      'Lift': 1, 'Threshold': 'NaN', 'type': 'NaN', 'var_name': var,'Selected':'N'}
                except:
                    cnt = mydata1[var].count()
                    data1 = mydata1[[var, target_sub]].dropna()
                    bad = sum(data1[target_sub] == 1)
                    badrate = bad / cnt
                    sample_bin = {'#Bad': bad, '#Obs': cnt, '%Bad_Rate': badrate, '%Obs': 1, 'Odds': 1,
                                  'Lift': 1, 'Threshold': 'NaN', 'type': 'NaN', 'var_name': var,'Selected':'N'}
                sample_bin = pd.DataFrame.from_dict(sample_bin, orient='index').T
                sample_bin['样本类型'] = sample_type_sub
                sample_bin['坏客户定义'] = target_sub
                sample_bin = sample_bin[
                    ['var_name', '样本类型', '坏客户定义', 'Threshold', 'type', '#Obs', '%Obs', '#Bad', '%Bad_Rate', 'Odds',
                     'Lift','Selected']]
                sample_bin.rename(columns={'%Bad_Rate': '%Bin_Bad_Rate', 'var_name': '变量英文名','Selected':'标签2'},  inplace=True)
                bin_summary = bin_summary.append(sample_bin)
    return bin_summary


# 规则效果分析和统计最终结果
def bin_result_summary_final(hit_num,bindata,var_select01,sub_div_bin,min_num,sample_type,method,numOfSplit):
    '''
    :param hit_num:       规则最小触碰量
    :param bindata:       传入需要分箱的数据
    :param var_select01:  变量描述性统计分析结果
    :param sub_div_bin:   头部和尾部样本占比
    :param min_num:       每个分箱的最小数量
    :param sample_type:   样本类型
    :param method:        分箱方法
    :param numOfSplit:    分箱个数
    :return:              最终分箱结果汇总
    '''
    bin_summary=pd.DataFrame(columns=['变量英文名', '样本类型', '坏客户定义', 'Threshold', 'type', '#Obs', '%Obs',
       '#Bad', '%Bin_Bad_Rate', 'Odds','Lift','标签2'])
    bin_summary_01=bin_result_summary(bindata=bindata,bin_summary=bin_summary,sample_type=sample_type,
                  sub_div_bin=sub_div_bin,min_num=min_num,hit_num=hit_num,method=method,numOfSplit=numOfSplit)
    bin_summary_select_var=['序号','分析时间','样本类型','坏客户定义','变量英文名','变量中文名',
                                     '样本区间','%Bad_Rate(不含缺失值)','%Bad_Rate(包含缺失值)','标签1']
    bin_summary_02=pd.merge(var_select01[bin_summary_select_var],bin_summary_01,on=['变量英文名','样本类型','坏客户定义'],how='left')
    ####添加标签3，此处标签3的结果等于标签2的结果
    filter2 = bin_summary_02
    filter2['标签3'] = 'N'
    pattern = re.compile(r'\d{1,2}')
    for sampletype in filter2.样本类型.unique():
        for target_ls in filter2.坏客户定义.unique():
            filter2_ls = filter2[(filter2.样本类型 == sampletype) & (filter2.坏客户定义 == target_ls)]
            for varname in set(filter2_ls.变量中文名[(filter2_ls.标签1 == 'Y') & (filter2_ls.标签2 == 'Y')]):
                data1 = filter2_ls[(filter2_ls.标签1 == 'Y') & (filter2_ls.标签2 == 'Y') & (filter2_ls.变量中文名 == varname)]
                if len(data1) == 1:
                    var = data1.变量英文名[data1.Odds == data1.Odds.max()].values[0]
                    filter2['标签3'][(filter2.标签1 == 'Y') & (filter2.标签2 == 'Y') & (filter2.变量英文名 == var) & (
                    filter2.坏客户定义 == target_ls)] = 'Y'
                if len(data1) > 1:
                    var1 = data1.变量英文名[data1.Odds == data1.Odds.max()].values[0]
                    filter2['标签3'][(filter2.标签1 == 'Y') & (filter2.标签2 == 'Y') & (filter2.变量英文名 == var1) & (
                    filter2.坏客户定义 == target_ls)] = 'Y'
    filter2 = filter2[['序号', '分析时间', '样本类型', '坏客户定义', '变量英文名', '变量中文名',
                       '样本区间', '%Bad_Rate(不含缺失值)', '%Bad_Rate(包含缺失值)', 'Threshold', 'type', '#Obs', '%Obs',
                       '#Bad', '%Bin_Bad_Rate', 'Odds', 'Lift', '标签1', '标签2', '标签3']]
    return filter2

# 分箱结果明细
def bin_result_detail(bindata,var_select02,sample_type,sub_div_bin,min_num,method,numOfSplit):
    '''
    :param my_data:       需要分析的数据
    :param var_select02:  filter2
    :param sample_type:   样本类型
    :param sub_div_bin:   头部和尾部分箱占比
    :param min_num:       每个分箱的最小数量
    :param method:        best 根据业务逻辑分箱；equalfre 等频率分箱；equallen等宽分箱
    :param numOfSplit:    等宽或者等频分箱数
    :param return:        分箱结果明细
    '''
    bin_summary_details=pd.DataFrame(columns=['序号','分析时间','样本类型','坏客户定义','变量英文名','变量中文名','样本区间','标签1', '标签2', '标签3', 'Bin', '#Obs', '%Obs','#Cum_Obs',
                                            '%Cum_Obs', '#Good', '%Good',  '#Cum_Good', '%Cum_Good', '#Bad', '%Bad', '#Cum_Bad',
                                            '%Cum_Bad','%Bad_Rate', 'WOE', 'IV(bin)', 'IV(total)', 'Odds1', 'Odds2','Lift'])
    for sample_type_sub in sample_type:
        print('正在分析的样本类型为：', sample_type_sub)
        if '其他' in sample_type_sub:
            describedata = bindata[bindata[sample_type_col[sample_type_sub][0]].map(
                lambda x: str(x) not in sample_type_col[sample_type_sub][1])]
        elif 'Total' in sample_type_sub:
            describedata = bindata
        else:
            describedata = bindata[bindata[sample_type_col[sample_type_sub][0]].map(
                lambda x: str(x) in sample_type_col[sample_type_sub][1])]
        target = sample_type_target[sample_type_sub]
        for target_sub in target:
            mydata1 = describedata[describedata[target_ripe[target_sub][0]] == 1]  ##获取成熟样本
            mydata1 = mydata1.drop(labels=target_del_col[target_sub], axis=1)
            min_rate = target_min_rate[target_sub][0]
            print(sample_type_sub, target_sub, '每箱最小占比：', min_rate, '数据量：', len(mydata1))
            for var in mydata1.columns[:-1]:
                print('正在分析变量:', var)
                try:
                    sample_bin = get_bin_lift(data=mydata1, flag_name=target_sub, factor_name=var,
                                                   min_rate=min_rate, sub_div_bin=sub_div_bin, min_num=min_num,
                                                   method=method,numOfSplit=numOfSplit)
                except:
                    value = ['(-inf,inf)', len(mydata1), 1, len(mydata1), 1, sum(mydata1[target_sub] == 0), 1,
                             sum(mydata1[target_sub] == 0), 1,
                             sum(mydata1[target_sub] == 1), 1, sum(mydata1[target_sub] == 1), 1,
                             sum(mydata1[target_sub] == 1) / len(mydata1),
                             np.NaN, np.NaN, np.NaN, 1, 1, 1]
                    sample_bin = pd.DataFrame([value],
                                              columns=['Bin', '#Obs', '%Obs', '#Cum_Obs', '%Cum_Obs', '#Good', '%Good',
                                                       '#Cum_Good', '%Cum_Good', '#Bad', '%Bad', '#Cum_Bad', '%Cum_Bad',
                                                       '%Bad_Rate', 'WOE', 'IV(bin)', 'IV(total)', 'Odds1', 'Odds2',
                                                       'Lift'])
                if len(sample_bin) <= 1:
                    value = ['(-inf,inf)', len(mydata1), 1, len(mydata1), 1, sum(mydata1[target_sub] == 0), 1,
                             sum(mydata1[target_sub] == 0), 1,
                             sum(mydata1[target_sub] == 1), 1, sum(mydata1[target_sub] == 1), 1,
                             sum(mydata1[target_sub] == 1) / len(mydata1),np.NaN, np.NaN, np.NaN, 1, 1, 1]
                    sample_bin = pd.DataFrame([value],
                                              columns=['Bin', '#Obs', '%Obs', '#Cum_Obs', '%Cum_Obs', '#Good', '%Good',
                                                       '#Cum_Good', '%Cum_Good', '#Bad', '%Bad', '#Cum_Bad', '%Cum_Bad',
                                                       '%Bad_Rate', 'WOE', 'IV(bin)', 'IV(total)', 'Odds1', 'Odds2',
                                                       'Lift'])
                sample_bin['变量英文名'] = var
                sample_bin['样本类型'] = sample_type_sub
                sample_bin['坏客户定义'] = target_sub
                var_msg = var_select02.loc[(var_select02.样本类型 == sample_type_sub) & (var_select02.变量英文名 == var) & (
                var_select02.坏客户定义 == target_sub),['序号', '分析时间', '样本类型', '坏客户定义', '变量英文名', '变量中文名',
                                '样本区间', '标签1', '标签2', '标签3']]
                merge = pd.merge(var_msg, sample_bin, on=['变量英文名', '样本类型', '坏客户定义'], how='left')
                bin_summary_details = bin_summary_details.append(merge)
    return bin_summary_details

'''
4. 变量分析步骤说明及分析结果统计
'''

def get_summary(filter2):
    '''
    :param filter2: 变量效果分析和筛选的数据
    :return: 泛化变量汇总信息
    '''
    aa=filter2
    ##此方法要保证变量类型唯一 ，否则的话    summary_info=aa[['变量开发','信息大类','信息类型','变量类型']].drop_duplicates().reset_index()

    summary_info=aa[['样本类型','坏客户定义']].drop_duplicates().reset_index()
    summary_info.drop(labels='index',inplace=True,axis=1)

    var_num=[]
    var_num_label1=[]
    var_num_label2=[]
    var_num_label3=[]
    for row in summary_info.iterrows():
        data=aa[(aa.样本类型==row[1][0]) &(aa.坏客户定义==row[1][1])]
        var_num.append(len(data))
        data1=aa[(aa.样本类型==row[1][0]) &(aa.坏客户定义==row[1][1]) &(aa.标签1=='Y')]
        var_num_label1.append(len(data1))
        data2 = aa[(aa.样本类型==row[1][0]) &(aa.坏客户定义==row[1][1])&(aa.标签1 == 'Y') & (aa.标签2 == 'Y')]
        var_num_label2.append(len(data2))
        data3 = aa[(aa.样本类型==row[1][0]) &(aa.坏客户定义==row[1][1])& (aa.标签1 == 'Y') & (aa.标签2 == 'Y') &(aa.标签3 == 'Y')]
        var_num_label3.append(len(data3))

    summary_info['变量总数'] = var_num
    summary_info['标签1筛选变量数'] = var_num_label1
    summary_info['标签1剔除变量数'] = summary_info['变量总数'] - summary_info['标签1筛选变量数']
    summary_info['标签2筛选变量数'] = var_num_label2
    summary_info['标签2剔除变量数'] = summary_info['标签1筛选变量数'] - summary_info['标签2筛选变量数']
    summary_info['标签3筛选变量数'] = var_num_label3
    summary_info['标签3剔除变量数'] = summary_info['标签2筛选变量数'] - summary_info['标签3筛选变量数']
    summary_info['剩余变量占比'] = summary_info['标签3筛选变量数'] / summary_info['变量总数']

    ###最终汇总结果输出
    summary_info = summary_info[['样本类型','坏客户定义', '变量总数',
                                 '标签1剔除变量数', '标签1筛选变量数',  '标签2剔除变量数', '标签2筛选变量数',
                                  '标签3剔除变量数', '标签3筛选变量数',  '剩余变量占比']]
    summary_info = summary_info.reset_index()
    del summary_info['index']
    sp = summary_info.sum().tolist()[2:-1]
    k1 = ['总计'] + ['']+sp + [sp[-1] / sp[0] if sp[0] > 0 else ['总计'] + sp + [0]]
    summary_info.loc[len(summary_info)] = k1
    return summary_info

