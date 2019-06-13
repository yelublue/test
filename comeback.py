import time
import pandas as pd
import numpy as np  
import datetime 
import math
import itertools
import talib
import shutil

today = '2019-06-11'
def init(context):
    # 在context中保存全局变量
    context.stocks = all_instruments(type='CS').order_book_id
    # context.stocks = ['600104.XSHG','002430.XSHE']
    context.waitingList1 = []
    context._5 = 5
    context._10 = 10
    context._20 = 20
    context._30 = 30
    context._60 = 60
    context._120 = 120
    context._250 = 250
    context.period = 10
    context.time=pd.DataFrame()
    context.dd1 = {'代码':[],'日期1':[],'日期2':[],'成交额':[],'高点距离':[]}
    context.dd2 = {'代码':[],'日期1':[],'日期2':[],'成交额':[],'高点距离':[]}
    context.dd3 = {'代码':[],'日期1':[],'日期2':[],'成交额':[],'高点距离':[]}
    
def before_trading(context):
    pass
def handle_bar(context, bar_dict):
    for s in context.stocks:
        # s = '300546.XSHE'
        # s = '603043.XSHG'
        # print(s)
        close = history_bars(s,context._250+1,'1d','close')
        high = history_bars(s,context._250+1,'1d','high')
        low = history_bars(s,context._250+1,'1d','low')
        date_time = history_bars(s,context._250+1,'1d','datetime')   #250天的收盘，最高，最低，日期ndarry
        amount = history_bars(s,context._250+1,'1d','total_turnover')
        close_1 = history_bars(s,371,'1d','close')
        if close is None:
            continue
        ma_5=talib.SMA(close, context._5)
        ma_10=talib.SMA(close, context._10)
        ma_20=talib.SMA(close, context._20)
        ma_30=talib.SMA(close, context._30)
        ma_60=talib.SMA(close, context._60)
        ma_120=talib.SMA(close_1, context._120)
        ma_250=talib.SMA(close, context._250)
        if len(close) > 5:
            
            ma5_amount = (amount[-1]+amount[-2]+amount[-3]+amount[-4]+amount[-5])/5
            low_min = np.min(low)      #250天内最低价
            low_index = np.where(low == low_min)[0][0]      #最低价出现的ndarray位置  int  
            high_after = high[low_index:]
            high_max = np.max(high_after)    #低点出现过后的最高价
            high_index = np.where(high_after == high_max)[0][0] + low_index  #最高价出现的ndarray位置  int
            
            # print(str(date_time[high_index])[0:8]
            case_1 = (high_index - low_index) <=250 and (high_index - low_index)>=10 #相差天数在10-120天内
            if high_max/low_min >= 1.5 and  case_1:
                
                #10-120天内涨幅超过50%,并且最高价和当前日期差5天以上@case2
                second_low = np.min(low[high_index:])      #找出调整后的最低点
                second_low_index_tmp = np.where(low[high_index:] == second_low)[0][0]#第二个低点在小np中的相对位置   
                second_low_index = second_low_index_tmp + high_index  #第二个低点在小np中的绝对位置
                if high_max/ second_low <= 1.42 and high_max / second_low >= 1.1 and second_low_index_tmp > 2: 
                    # print('1122')
                    #调整幅度不超过1.42，超过10%，并且调整时间超过2天@case3
                    high_date = str(date_time[high_index])[0:8]                           #高点出现日期   
                    second_low_date = str(date_time[second_low_index])[0:8]           #第二个低点的日期
                    
                    
                    # if second_low_date == str(context.now.strftime('%Y%m%d')):#制作碰低点的dic
                    if context.now.strftime('%Y-%m-%d') == today:
                        context.dd2['日期1'].append(high_date)
                        context.dd2['日期2'].append(second_low_date)
                        context.dd2['代码'].append(s)
                        context.dd2['成交额'].append(ma5_amount/100000000)
                        context.dd2['高点距离'].append(len(date_time) - high_index) 
                            
                        # print('加入%s'%s)
                        
                        
                    if second_low > ma_120[-1]*0.95 and second_low <= ma_120[-1]*1.1:#制作120上下的dic

                        # if second_low_date == str(context.now.strftime('%Y%m%d')):
                        if context.now.strftime('%Y-%m-%d') == today:
                            context.dd3['日期1'].append(high_date)
                            context.dd3['日期2'].append(second_low_date)
                            context.dd3['代码'].append(s)
                            context.dd3['成交额'].append(ma5_amount/100000000)
                            context.dd3['高点距离'].append(len(date_time) - high_index)   
                            
                    if len(ma_120) > 119:    
                        # print('1221')
                        #制作突破的dic
                        ma_120 = ma_120[119:-1]
                        if len(ma_120)>second_low_index:
                                # print('1212')
                            # if ma_120[-2] > ma_250[-2]*1.05:#120天线比250天线高5%
                            # if ma_120[second_low_index]*1.1 > close[second_low_index] and ma_120[second_low_index]*0.9 < close[second_low_index]:
                                
                                if not (close[-2]>ma_5[-2] and close[-2] >ma_10[-2] and close[-2]>ma_20[-2] and close[-2]>ma_30[-2] and close[-2]>ma_60[-2]  ): #前一天不在五线之上
                                    
                                    if close[-1] > ma_5[-1] and close[-1] > ma_10[-1] and close[-1] > ma_20[-1] and close[-1] > ma_30[-1] and close[-1] > ma_60[-1]:#今天站上五线
                                        # if close[-1] < ma_120[-1]*1.1:#收盘价比120日线的10%以内
                                        
                                            # if close[-1]>ma_120[-1] and close[-1] > ma_250[-1]:#收盘价比120和250大

                                            context.dd1['日期1'].append(high_date)
                                            context.dd1['日期2'].append(context.now.strftime('%Y-%m-%d'))
                                            context.dd1['代码'].append(s)
                                            context.dd1['成交额'].append(ma5_amount/100000000)
                                            context.dd1['高点距离'].append(len(date_time) - high_index)                         

# after_trading函数会在每天交易结束后被调用，当天只会被调用一次
def after_trading(context):
    print(context.now.strftime('%Y-%m-%d'))
    if context.now.strftime('%Y-%m-%d') == today:
        
        n = 1
        for i in [context.dd1,context.dd2,context.dd3]:
            data = pd.DataFrame(i)
            def change_code(x):
                if x.endswith('.XSHG'):
                    x = x.replace('.XSHG','.SH')
                    return x

                else:
                    x = x.replace('.XSHE','.SZ')
                    return x
            data['代码'] = data['代码'].apply(change_code)
            data['日期1'] = pd.to_datetime(data['日期1']).dt.date
            data['日期2'] = pd.to_datetime(data['日期2']).dt.date
            data['成交额'] = data['成交额'].round(2)
            data = data[['代码','日期1','日期2','成交额','高点距离']]
            data = data.sort_values('日期2',ascending = False)
            # print(data)
            if n == 1:
                data_1 = data
            elif n == 2:
                data_2 = data
            elif n == 3:
                data_3 = data
            else:
                continue
            n += 1
        writer = pd.ExcelWriter('1_rice_raw.xls')
        data_1.to_excel(writer,'突破')
        data_2.to_excel(writer,'回调')
        data_3.to_excel(writer,'回调120')
        writer.save()
        # print('done')
        writer = pd.ExcelWriter('%s_rice_back.xls'%(str(datetime.datetime.now())[0:10]))
        for n in ['突破','回调120','回调']:

            data_f = pd.read_excel('baobiao.xlsx')   #读取整个市场财务报表

            data_d = pd.read_excel('1_rice_raw.xls',encoding = 'gbk',sheetname = n) #读取每天选股结果

            data_d = data_d[['代码', '日期1', '日期2','成交额','高点距离']]
            
            print(data_d.columns)
            data_d.columns = ['code','date_1','date_2','成交额','高点距离']

            data_f = data_f[data_f.columns[0:12]]
            data_f.columns = ['code', 'name', 'cashflow', 'kind_1',
                   'kind_all', 'date_to_market', 'date_to_middle',
                   'for_middle',
                   'true_middle','5d_turnover','5d_amount','概念']   #更改两个表格的列名


            df = pd.merge(data_f,data_d,on = 'code',how = 'right')  #向每天选股表格合并数据

            df = df[['code', 'name', 'cashflow', 'kind_1', 'kind_all','概念', 'date_to_market',
                   'date_to_middle', 'for_middle', 'true_middle', '5d_turnover',
                   '成交额', 'date_1', 'date_2','高点距离']]
            df['date_1'] = df['date_1'].dt.date
            df['date_2'] = df['date_2'].dt.date 
            df3 = df.sort_values('date_2',ascending = False)
            df1 = df[(df.for_middle > 30) | (df.true_middle > 30)]
            df1 = df1.sort_values('date_2',ascending = False)
            df3.to_excel(writer,n)
            df1.to_excel(writer,n+'_预增')

        writer.save()
        print('done')
        shutil.copy('%s_rice_back.xls'%(str(datetime.datetime.now())[0:10]),'D:/微云同步助手/191923833/阿天/选股结果/%s_rice_back.xls'%(str(datetime.datetime.now())[0:10]))
        #复制到网盘里
        print('copy done')