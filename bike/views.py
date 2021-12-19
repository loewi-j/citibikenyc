from django.shortcuts import render
from django.http import HttpResponse
from django.http.response import JsonResponse
from django.shortcuts import render
import json
import numpy as np
import pandas as pd
import os

march_daily = pd.read_csv(os.getcwd() + '\\bike\\data\\march_daily_borrow.csv')
week_hour = pd.read_csv(os.getcwd() + '\\bike\\data\\march_week_hour_borrow.csv')
work_hour = pd.read_csv(os.getcwd() + '\\bike\\data\\march_work_hour_borrow.csv')
space_kmeans_2020 = pd.read_csv(os.getcwd() + '\\bike\\data\\space_kmeans_2020.csv')
space_kmeans_2019 = pd.read_csv(os.getcwd() + '\\bike\\data\\space_kmeans_2019.csv')


# traffic_info = pd.read_json(os.getcwd() + '\\bike\\data\\traffic_info_20_2020.json')
# station_info = pd.read_json(os.getcwd() + '\\bike\\data\\kc_station_info_20_2020.json')


# Create your views here.
def daily_borrow(request):
    print(week_hour)
    return_data = {
        "status": 1,
        # "message": "Raw Data List with pagination",
        "data": {
            "daily": json.loads(march_daily.T.to_json()),
            "week": json.loads(week_hour.T.to_json()),
            "work": json.loads(work_hour.T.to_json())
        }
    }
    return HttpResponse(json.dumps(return_data), content_type='application/json')
    # return HttpResponse(json.dumps(march_daily), content_type='application/json')


def getDetail(request):
    pd.set_option('display.width', None)
    date1 = request.GET['date1']
    date2 = request.GET['date2']
    kmean1 = int(request.GET['kmean1'])
    kmean2 = int(request.GET['kmean2'])
    kNumber = request.GET['kNumber']
    year = request.GET['year']
    print(date1, date2, kmean1, kmean2, kNumber, year)

    traffic_info = pd.read_json(os.getcwd() + '\\bike\\data\\traffic_info_' + kNumber + '_' + year + '.json')
    # print(traffic_info)
    station_info = pd.read_json(os.getcwd() + '\\bike\\data\\kc_station_info_' + kNumber + '_' + year + '.json')
    sites_only = pd.read_csv(os.getcwd() + '\\bike\\data\\sites_only_' + year + '.csv')

    # 筛选时间
    select_day_traffic = traffic_info[
        (traffic_info['starttime'] >= date1) & (traffic_info['starttime'] <= date2)]

    # 展开矩阵
    item_data = pd.DataFrame(None)
    for tup in select_day_traffic.itertuples():
        temp = pd.DataFrame(select_day_traffic['item'][tup[0]])
        item_data = pd.concat([item_data, temp], ignore_index=True)
    # 筛选起始聚类
    select_kmean_item = item_data[
        (item_data['form'] == kmean1) & (item_data['to'] == kmean2)]
    # 统计总流量
    sum_data = select_kmean_item['size'].sum()

    # 再次展开矩阵
    detail_data = pd.DataFrame(None)
    for tup in select_kmean_item.itertuples():
        temp = pd.DataFrame(select_kmean_item['detail_trip'][tup[0]])
        detail_data = pd.concat([detail_data, temp], ignore_index=True)

    # 根据起始站点和结束站点分类 统计每条路线权重
    detail_sum_data = pd.DataFrame(detail_data.groupby(['form', 'to']).agg({'size': sum})).reset_index()

    # 与站点名称合并
    detail_sum_data = pd.merge(detail_sum_data, sites_only, how='outer', left_on='to', right_on='start station id')
    # 修改列名
    detail_sum_data.rename(columns={'start station name': 'end station name'}, inplace=True)  # 修改终点聚类列名
    detail_sum_data = pd.merge(detail_sum_data, sites_only, how='outer', left_on='form', right_on='start station id')
    detail_sum_data.dropna(axis=0, how='any', inplace=True)  # 去除nan
    # print(detail_sum_data)
    detail_sum_data.rename(columns={'start station latitude_y': 'start station latitude',
                                    'start station longitude_y': 'start station longitude'}, inplace=True)  # 修改终点聚类列名
    detail_sum_data = detail_sum_data[
        ['form', 'to', 'end station name', 'start station name', 'size', 'start station latitude',
         'start station longitude']]

    station_data = pd.DataFrame(None)
    # 如果是散点点击 计算当前聚类所包含的站点
    if kmean1 == kmean2:
        station_info_select = station_info[station_info['id'] == kmean1]

        # 展开矩阵
        station_item_data = pd.DataFrame(None)
        for tup in station_info_select.itertuples():
            temp = pd.DataFrame(station_info_select['detail'][tup[0]])
            station_item_data = pd.concat([station_item_data, temp], ignore_index=True)

        # 与站点具体信息合并
        station_data = pd.merge(station_item_data, sites_only, how='outer', left_on='id', right_on='start station id')
        station_data.dropna(axis=0, how='any', inplace=True)  # 去除nan

    return_data = {
        "status": 1,
        # "message": "Raw Data List with pagination",
        "data": {
            "sum": sum_data,
            "detail": json.loads(detail_sum_data.T.to_json()),
            "self": json.loads(station_data.T.to_json()),
            "self_sum": json.loads(str(station_data.shape[0])),
        }
    }
    return HttpResponse(json.dumps(return_data), content_type='application/json')


def kmean_merge(request):
    # 根据前端传入数据分析
    date1 = request.GET['date1']
    date2 = request.GET['date2']
    traffic1 = int(request.GET['traffic1'])
    traffic2 = int(request.GET['traffic2'])
    kNumber = request.GET['kNumber']
    year = request.GET['year']
    print(date1, date2, traffic1, traffic2, kNumber, year)

    merge_data = pd.read_csv(os.getcwd() + '\\bike\\data\\merge_data_size_' + kNumber + '_' + year + '.csv')
    # print(merge_data)

    # 筛选时间
    select_day_station = merge_data[(merge_data['starttime'] >= date1) & (merge_data['starttime'] <= date2)]
    # print(select_day_station)

    # 根据起始站点和结束站点分类 统计每条路线权重
    sum_data = pd.DataFrame(select_day_station.groupby(['kmeans_id_start', 'kmeans_id_end', 'end station latitude',
                                                        'end station longitude', 'start station latitude',
                                                        'start station longitude']).agg({'size': sum})).reset_index()
    # print(sum_data)

    # 筛选自流量
    self_data = sum_data[sum_data['kmeans_id_start'] == sum_data['kmeans_id_end']]
    # print(self_data)

    # 筛选自定义流量路径：
    sum_data = pd.DataFrame(sum_data[(sum_data['size'] >= traffic1) &
                                     (sum_data['size'] <= traffic2)]).reset_index(drop=True)
    size_list = sum_data['size'].values
    max_size = size_list.max()
    min_size = size_list.min()

    unSelf_data = sum_data[sum_data['kmeans_id_start'] != sum_data['kmeans_id_end']]
    size_list = unSelf_data['size'].values

    unSelf_data['level'] = (unSelf_data['size'] - min_size) / (max_size - min_size) * 3 + 1
    unSelf_data = unSelf_data.sort_values(by='size')
    # print(unSelf_data)

    size_list = self_data['size'].values
    self_data['level'] = (self_data['size'] - min_size) / (max_size - min_size) * 10 + 4
    # 将负数流量赋值为0
    self_data.loc[self_data[self_data['level'] < 0].index.tolist(), 'level'] = 0

    return_data = {
        "status": 1,
        # "message": "Raw Data List with pagination",
        "data": {
            "coordinates": json.loads(unSelf_data.T.to_json()),
            "self_data": json.loads(self_data.T.to_json()),
            "max_size": max_size,
            "min_size": min_size,
        }
    }
    return HttpResponse(json.dumps(return_data), content_type='application/json')


def getLevel(list):
    # 取分位数
    level = np.quantile(list, [.2, .4, .6, .8, .85, .9, .93, .96, .99])

    level_list = []
    flag = False
    # 遍历list
    for row in list:
        # 与分位数比较
        for i, item in enumerate(level):
            if row <= item:
                level_list.append(i)
                # 设置标志
                flag = True
                break
        # > level[8]时
        if not flag:
            level_list.append(9)
        flag = False
    return level_list


def getSpaceTime(request):
    print(space_kmeans_2020)
    print(space_kmeans_2019)
    return_data = {
        "status": 1,
        # "message": "Raw Data List with pagination",
        "data": {
            "time_20": json.loads(space_kmeans_2020.T.to_json()),
            "time_19": json.loads(space_kmeans_2019.T.to_json()),
        }
    }
    return HttpResponse(json.dumps(return_data), content_type='application/json')

def getSpaceMapData(request):
    print(request)
    kmeans_id = request.GET['kmeans_id']
    kNumber = request.GET['kNumber']
    print(kmeans_id, kNumber)
    # 获取该聚类日期
    space_data = space_kmeans_2020[space_kmeans_2020['kmeans_id'] == kmeans_id]
    print(space_data)

    merge_data = pd.read_csv(os.getcwd() + '\\bike\\data\\space_merge_2020_' + kmeans_id + '.csv')

    # 筛选自流量
    self_data = merge_data[merge_data['startClusterID'] == merge_data['endClusterID']]

    # 筛选自定义流量路径：
    num_list = merge_data['num'].values
    max_num = num_list.max()
    min_num = num_list.min()
    print(max_num, min_num)

    unSelf_data = merge_data[merge_data['startClusterID'] != merge_data['endClusterID']]
    unSelf_data['level'] = (unSelf_data['num'] - min_num) / (max_num - min_num) * 3 + 1
    unSelf_data = pd.DataFrame(unSelf_data[(unSelf_data['level'] >= 1.4)]).reset_index(drop=True)

    self_data['level'] = (self_data['num'] - min_num) / (max_num - min_num) * 10 + 4
    # 将负数流量赋值为0
    self_data.loc[self_data[self_data['level'] < 0].index.tolist(), 'level'] = 0

    num_list = unSelf_data['num'].values
    # max_num = num_list.max()
    min_num = num_list.min()

    return_data = {
        "status": 1,
        # "message": "Raw Data List with pagination",
        "data": {
            "space_data": json.loads(unSelf_data.T.to_json()),
            "self_data": json.loads(self_data.T.to_json()),
            "max_size": max_num,
            "min_size": min_num,
        }
    }
    return HttpResponse(json.dumps(return_data), content_type='application/json')

