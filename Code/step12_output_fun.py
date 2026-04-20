#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/3/4 20:21
# @Author  : Feng Zhanpeng
# @File    : step12_output_fun.py
# @Software: PyCharm

"""
单维度策略测算部分计算结果输出相关函数，直接加载即可
"""

import copy

'''
xlsxwriter格式设置和配色设置
'''
# 配色
biaotou='#366092'
text='#F4F4F4'
title='#44546A'
title_sub1='#00868B'
title_sub2='#00E5EE'

# 通用搭配
split_color='#FFFFFF'   #白色
xunhuan1='#D1D1D1'      #银色
xunhuan2='#E3E3E3'      #银色
title_size=12
biaotou_size=10
text_size=8

# 条件格式: 蓝色数据条，不指定最大、最小值，绿色
condition_format_green_no = {'type': 'data_bar','bar_solid': True,'data_bar_2010': True,
                            'bar_color': '#65d97d'}
# 条件格式: 黄色数据条，不指定最大、最小值，橙色
condition_format_yellow_no = {'type': 'data_bar','bar_solid': True,'data_bar_2010': True,
                              'bar_color': '#f2572d'}
# 条件格式: 黄色数据条，不指定最大、最小值，橙色
condition_format_pink_no = {'type': 'data_bar','bar_solid': True,'data_bar_2010': True,
                            'bar_color': '#FF69B4'}
condition_format_blue_no = {'type': 'data_bar','bar_solid': True,'data_bar_2010': True,
                            'bar_color': '#1E90FF'}

# 总标题格式
title_format = {'bold': True,'font_name': 'Arial','font_size': title_size,'font_color': 'white',
               'top_color': biaotou,'bottom_color': biaotou,'left_color': biaotou,
              'right_color': biaotou,'bg_color': biaotou}

# 副标题格式
subtitle_format={'border': True,'font_size': biaotou_size,'font_name': 'Arial',
                               'top_color':title,'font_color': 'white',
                              'bottom_color': title,'bold': True,
                               'left_color': split_color,
                               'right_color': split_color,
                               'bg_color': title,
                               'align': 'left',
                               'valign': 'vcenter'}
# 表格正文
body_text_format_01={'border': True,'font_size': text_size,'font_name': 'Arial',
                               'top_color':xunhuan1,
                               'bottom_color': xunhuan1,
                               'left_color': split_color,
                               'right_color': split_color,
                               'bg_color': xunhuan1,
                               'align': 'left',
                               'valign': 'vcenter'}
# 正文为百分比
body_text_per_format_01=copy.deepcopy(body_text_format_01)
body_text_per_format_01['num_format']='0.00%'

# 表格正文
body_text_format_02={'border': True,'font_size': text_size,'font_name': 'Arial',   #border：边框
                               'top_color':xunhuan2,
                               'bottom_color': xunhuan2,
                               'left_color': split_color,
                               'right_color':split_color,
                               'bg_color': xunhuan2,
                               'align': 'left',
                               'valign': 'vcenter'}

# 正文为百分比
body_text_per_format_02=copy.deepcopy(body_text_format_02)
body_text_per_format_02['num_format']='0.00%'

# 表格正文
body_text_format_03 = {'bold': True,'font_name': 'Arial','font_size':text_size,'font_color': 'black'}


'''
标准化结果输出函数
'''

# 自动获取单元格内容的length，如果包含汉字，则一个汉字计数为2，如果是英文，则计数为1
def get_same_len(x):
    """
    :param x:       输入内容
    :return:        字符串长度
    """
    import re
    if type(x)!=str:
        x=str(x)
    l=list(x)
    num=0
    for i in l:
        if re.match("[\u4e00-\u9fa5]+",i):
            num=num+2
        else:
            num=num+1
    return num

'''
变量分箱结果明细输出函数
'''

def  details_result_output(wb,sheetname,data,suoyin,ana_people):
    '''
    :param wb:         excel 文件
    :param sheetname:  sheetname
    :param data:       待输出数据
    :param suoyin:     序号所在的列
    :param ana_people: 分析人
    :return:
    '''
    nrows, ncols = data.shape
    body_text_xunhuan1 = wb.add_format(body_text_format_01)
    body_text_xunhuan1_per = wb.add_format(body_text_per_format_01)
    body_text_xunhuan2 = wb.add_format(body_text_format_02)
    body_text_xunhuan2_per = wb.add_format(body_text_per_format_02)
    body_text_title = wb.add_format(subtitle_format)
    ws = wb.add_worksheet(sheetname)
    ws.freeze_panes(1, 4)  ## 冻结单元格
    ws.autofilter(0,0,nrows,ncols-1)
    ws.hide_gridlines({'option': 1})
    column = data.columns
    for i in range(len(column)):
        x = column[[i]][0]
        ll = get_same_len(x)
        lll = max(8, ll)
        ws.set_column(i , i , lll)
    data = data.replace(np.inf, 'inf')
    data = data.fillna('')
    data = data.replace(-np.inf, '-inf')
    for j in range(ncols):
        ws.write(0, j, column[j], body_text_title)
    for i in range(nrows):
        for j in range(ncols):
            if 'pass1' in column[j]:
                ws.conditional_format(1, j, nrows, j, condition_format_green_no)
            elif '%Bad_Rate' in column[j] or 'Lift' in column[j]:
                ws.conditional_format(1, j, nrows, j, condition_format_pink_no)
            elif 'pass2' in column[j]:
                ws.conditional_format(1, j, nrows, j, condition_format_red_no)
            elif 'pass3' in column[j]:
                ws.conditional_format(1, j, nrows, j, condition_format_blue_no)
            value = data.iloc[i][j]
            key = int(data.iloc[i, suoyin].replace(ana_people, ''))
            if key % 2 == 1:
                if ('%' in column[j] or '率' in column[j] or 'Rate' in column[j] or column[j] in ['单一值最大占比', '单一值第二大占比',
                                                                                                 '单一值第三大占比',
                                                                                                 '单一值前二大占比总和',
                                                                                                 '单一值前三大占比总和']):
                    ws.write(i + 1, j, value, body_text_xunhuan1_per)
                else:
                    ws.write(i + 1, j, value, body_text_xunhuan1)
            else:
                if ('%' in column[j] or '率' in column[j] or 'Rate' in column[j] or column[j] in ['单一值最大占比', '单一值第二大占比',
                                                                                                 '单一值第三大占比',
                                                                                                 '单一值前二大占比总和',
                                                                                                 '单一值前三大占比总和']):
                    ws.write(i + 1, j, value, body_text_xunhuan2_per)
                else:
                    ws.write(i + 1, j, value, body_text_xunhuan2)

'''
分析结果汇总输出函数
'''
def summary_result_output(wb, sheetname, data):
    '''
    :param wb:         excel 文件
    :param sheetname:  sheetname
    :param data:       待输出数据
    :return:
    '''
    nrows, ncols = data.shape
    body_text_xunhuan1 = wb.add_format(body_text_format_01)
    body_text_xunhuan1_per = wb.add_format(body_text_per_format_01)
    body_text_xunhuan2 = wb.add_format(body_text_format_02)
    body_text_xunhuan2_per = wb.add_format(body_text_per_format_02)
    body_text_title = wb.add_format(subtitle_format)
    ws = wb.add_worksheet(sheetname)
    ws.freeze_panes(1, 4)  ## 冻结单元格
    ws.autofilter(0, 0, nrows, ncols - 1)
    ws.hide_gridlines({'option': 1})
    column = data.columns
    for i in range(len(column)):
        x = column[[i]][0]
        ll = get_same_len(x)
        lll = max(8, ll)
        ws.set_column(i, i, lll)
    data = data.replace(np.nan, '')
    data = data.replace(np.inf, 'Inf')
    for j in range(ncols):
        ws.write(0, j, column[j], body_text_title)
    for i in range(nrows):
        for j in range(ncols):
            value = data.iloc[i][j]
            if i % 2 == 1:
                if ('%' in column[j] or '率' in column[j] or column[j] in ['单一值最大占比', '单一值第二大占比',
                                                                          '单一值第三大占比',
                                                                          '单一值前二大占比总和', '单一值前三大占比总和']):
                    ws.write(i + 1, j, value, body_text_xunhuan1_per)
                else:
                    ws.write(i + 1, j, value, body_text_xunhuan1)
            else:
                if ('%' in column[j] or '率' in column[j] or column[j] in ['单一值最大占比', '单一值第二大占比',
                                                                          '单一值第三大占比',
                                                                          '单一值前二大占比总和', '单一值前三大占比总和']):
                    ws.write(i + 1, j, value, body_text_xunhuan2_per)
                else:
                    ws.write(i + 1, j, value, body_text_xunhuan2)

'''
第4步：策略测算过程及变量筛选情况
'''

def  var_summary_result_output(wb,sheetname,data,start=0):
    '''
    :param wb:         excel 文件
    :param sheetname:  sheetname
    :param data:       待输出数据
    :param start:      开始进行输出的表格行
    :return:
    '''
    ws = wb.add_worksheet(sheetname)
    column = data.columns
    nrows, ncols = data.shape
    for i in range(ncols):
        ws.set_column(i+1, i+1, 10)
    body_text_title = wb.add_format(subtitle_format)
    # sheet第四行，写入总标题
    body_title = wb.add_format(title_format)
    # 表格正文: 边框白色，字体12，背景浅灰色、居中
    body_text_xunhuan2 = wb.add_format(body_text_format_02)
    body_text_xunhuan2_per = wb.add_format(body_text_per_format_02)
    body_text_red = wb.add_format(subtitle_format)
    body_text_red.set_font_color('red')
    body_text_pink = wb.add_format(body_text_format_03)
    body_text_pink.set_bg_color('#F2DCE5')
    body_text_pink.set_text_wrap('True')
    body_text_pink.set_align('vcenter')
    body_text_blue = wb.add_format(body_text_format_03)
    body_text_blue.set_bg_color('#DAEEF3')
    body_text_red_xifen = wb.add_format(subtitle_format)
    body_text_red_xifen.set_font_color('red')
    ws.hide_gridlines({'option': 1})
    ws.merge_range(1 + start, 1, 1 + start, ncols, '一、' + sheetname, body_title)
    ws.set_column(0, 0, 2)
    ws.set_row(start + 3, 130)
    ws.set_row(start + 5, 100)
    ws.set_row(start + 7, 67)
    ws.set_row(start + 9, 26)
    ws.set_row(start + 10, 7)
    remark1 = '1.变量基础分析和筛选'
    remark2 = '''
           (1) 分析维度
           样本量、缺失量、缺失率、Badrate、单一值最大占比的变量值、单一值最大占比的样本量、单一值最大占比、单一值第二大占比的变量值、
           单一值第二大占比的样本量、单一值第二大占比、单一值第三大占比的变量值、单一值第三大占比的样本量、单一值第三大占比、
           单一值前二大占比的总样本量、单一值前二大占比总和、单一值前三大占比的总样本量、单一值前三大占比总和、变量取值数（包含缺失值）、
           变量取值数（不含缺失值）、最小值、最大值、平均值、分位数、标准差、离散系数
           (2) 筛选标准
           a.单一值最大占比 < 99%
           b.变量取值数（不含缺失值） >=2
           筛选结果详见标签1
           '''
    remark3 = '2.变量效果分析和筛选'
    remark4 = '''
           (1) 对变量进行分箱，计算不同分箱的触碰量、触碰率、Odds、Lift等指标
           (2) 基于头部和尾部分箱结果对变量进行筛选
           a.最小触碰量 >= 30 (备注：大于等于某一阈值，默认30)
           b.触碰率 <= 5% (备注：小于等于某一阈值，默认5%)
           c.Lift >= 3  (备注：大于等于某一阈值，默认3)
           筛选结果详见标签2
           '''
    remark5 = '3.变量相关性分析和筛选'
    remark6 = '''
           筛选标准
           a.对标签2筛选的变量进行两两线性相关分析，若相关性较强，选取Lift值大的变量
           b.选取有明确业务含义的变量
           筛选结果详见标签3
           '''
    remark7 = '4.变量分析结果汇总'
    sk0 = data['变量总数'][nrows - 1]
    sk1 = data['标签1筛选变量数'][nrows - 1]
    sk2 = data['标签2筛选变量数'][nrows - 1]
    sk3 = data['标签3筛选变量数'][nrows - 1]
    sk5 = float(data['剩余变量占比'][nrows - 1])
    remark8 = '''        分析的变量数总计为%s个，标签1筛选剩余%s个，标签2筛选剩余%s个，标签3筛选剩余%s个，最终筛选剩余变量占比为%.2f''' % (
        sk0, sk1, sk2, sk3, sk5 * 100) + '%'
    ws.merge_range(start + 2, 1, start + 2, ncols, remark1, body_text_blue)
    ws.merge_range(start + 3, 1, start + 3, ncols, remark2, body_text_pink)
    ws.merge_range(start + 4, 1, start + 4, ncols, remark3, body_text_blue)
    ws.merge_range(start + 5, 1, start + 5, ncols, remark4, body_text_pink)
    ws.merge_range(start + 6, 1, start + 6, ncols, remark5, body_text_blue)
    ws.merge_range(start + 7, 1, start + 7, ncols, remark6, body_text_pink)
    ws.merge_range(start + 8, 1, start + 8, ncols, remark7, body_text_blue)
    ws.merge_range(start + 9, 1, start + 9, ncols, remark8, body_text_pink)
    ws.merge_range(start + 10, 1, start + 10, ncols, '', body_text_pink)
    add = 8
    data = data.replace(np.inf, 'inf')
    data = data.fillna('')
    data = data.replace(-np.inf, '-inf')
    body_text_title.set_text_wrap('True')
    for j in range(ncols):
        ws.write(start + 3 + add, j + 1, column[j], body_text_title)
    ws.autofilter(start + 3 + add, 1, start + 3 + add, ncols)
    for i in range(nrows):
        for j in range(ncols):
            value = data.iloc[i][j]
            if ('%' in column[j] or '占比' in column[j] or 'rate' in column[j]):
                ws.write(i + start + 4 + add, j + 1, value, body_text_xunhuan2_per)
            else:
                ws.write(i + start + 4 + add, j + 1, value, body_text_xunhuan2)

'''
第6步：最终筛选变量结果输出
'''

def  var_summary_result_output_01(wb,sheetname,data):
    '''
    :param wb:         excel 文件
    :param sheetname:  sheetname
    :param data:       待输出数据
    :return:
    '''
    nrows, ncols = data.shape
    body_text_xunhuan2 = wb.add_format(body_text_format_02)
    body_text_xunhuan2_per = wb.add_format(body_text_per_format_02)
    body_text_title = wb.add_format(subtitle_format)
    ws = wb.add_worksheet(sheetname)
    ws.freeze_panes(1, 4)  ## 冻结单元格
    ws.autofilter(0,0,nrows,ncols-1)
    ws.hide_gridlines({'option': 1})
    column = data.columns
    for i in range(len(column)):
        x = column[[i]][0]
        ll = get_same_len(x)
        lll = max(8, ll)
        ws.set_column(i , i , lll)
    data = data.replace(np.nan, '')
    data = data.replace(np.inf, 'Inf')
    for j in range(ncols):
        ws.write(0 , j , column[j], body_text_title)
    for i in range(nrows):
        for j in range(ncols):
            value = data.iloc[i][j]
            if ('%' in column[j]  or '占比' in column[j]  or 'rate' in column[j] ):
                ws.write( i+1, j , value, body_text_xunhuan2_per)
            else:
                ws.write(i + 1, j, value, body_text_xunhuan2)

