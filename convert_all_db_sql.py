#!/usr/bin/python
# -*- coding: UTF-8 -*-
# Funtion: 实现分库分表操作
from __future__ import division

import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import sys
import MySQLdb
import time
import codecs

# 连接数据库
db = MySQLdb.connect("10.30.81.137", "analyse_tmp", "xxxxxxxxxxxxx", "dev_dgc")

cursor = db.cursor()

# 每次取50万条数据
step = 500000


def exec_sql(tmp_sql):
    cursor.execute(tmp_sql)
    return cursor.fetchall()


def new_write_result_to_file(_all_content):
    # return True
    for each_file_name, each_content in _all_content.items():
        with codecs.open(each_file_name, 'a+', encoding='utf-8-sig') as tmp_f:
            tmp_f.write(''.join(each_content))


all_content = {}


def resolve_data(start_id, end_id, _table_name, _max_id):
    # 在此确定分库分表后，1000张表的表名的标识
    for i in range(1, 11):
        for j in range(1, 101):
            all_content['new_player_device_{}_player_devices{}'.format(i, j)] = []
		
    if end_id == _max_id:
        # 分库分表的核心：mod(CRC32(new_udid),1000) ，这个值在1~999之间
        sql = 'select id,new_udid,player_id,mod(CRC32(new_udid),1000) as crs32 from %s where id >= %s and id <= %s and new_udid is not null;' % (
        _table_name, start_id, end_id)

    else:
        sql = 'select id,new_udid,player_id,mod(CRC32(new_udid),1000) as crs32 from %s where id >= %s and id < %s and new_udid is not null;' % (
        _table_name, start_id, end_id)
    results = exec_sql(sql)
    for row in results:
        new_udid = row[1]
        player_id = row[2]
        crc32 = '%03d' % row[3] #结果是001~999
        # content = new_udid + '|' + str(player_id) + '\n'
        # first_num用来作为库的标识
        first_num = crc32[0]
        # last_num用来作为表的标识
        last_num = crc32[1:]

        # 判断首位为0
        if not int(first_num):
            first_num = '10'

        # 判断末两位为00
        if not int(last_num):
            last_num = '100'
		
        file_name = 'new_player_device_' + first_num + '_player_devices%s' % int(last_num)

        content = 'insert ignore into new_player_device_%s.player_devices%s (new_udid,player_id) values (\'%s\',%s);' % (
        first_num, int(last_num), new_udid, player_id) + '\n'
        all_content[file_name].append(content)

    new_write_result_to_file(all_content)
    log = '\rtable_name: %s, total: %s, completed: %s ,now at %s' % (
    _table_name, _max_id, str(round(end_id / _max_id * 100, 2)) + '%', end_id)
    sys.stdout.write(log)
    sys.stdout.flush()


def resolve(_table_name, _max_id):
    start_id = 0
    _start_time = time.time()
    end_id = start_id + step
    # 考虑到表数据过大，每张表以迭代的形式轮询取 start_id ~ end_id 范围的数据
    while end_id < _max_id:
        resolve_data(start_id, end_id, _table_name, _max_id)
        start_id = end_id
        end_id = end_id + step

    resolve_data(start_id, _max_id, _table_name, _max_id)

    _end_time = time.time()
    print '\ntable_name:%s ,completed ,total time:%s\n' % (_table_name, _end_time - _start_time)


# 现有的20个表名
table_list = ['player_devices%s' % table_num for table_num in range(1, 21)]

if __name__ == '__main__':
    start_time = time.time()
    print 'start', time.ctime(), start_time

    for each_table_name in table_list:
        max_id_sql = 'select max(id) from %s;' % each_table_name

        max_id = exec_sql(max_id_sql)[0][0]
        resolve(each_table_name, max_id)
    end_time = time.time()
    print time.ctime(), end_time
    print 'all completed total time:', end_time - start_time
    db.close()
