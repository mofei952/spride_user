#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Author  : mofei
# @Time    : 2018/10/3 10:17
# @File    : hxqb2.py
# @Software: PyCharm

import base64
import calendar
import copy
import datetime
import json
import os
import queue
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, ALL_COMPLETED, wait
from queue import Queue

import requests
from lxml import etree
from xlrd import open_workbook
from xlwt import Workbook

base_url = 'http://hxqb.xiazhuanke.com'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
    'Cookie': '_9755xjdesxxd_=32; USER.ACCOUNT=aHhxYg==; gdxidpyhxdE=s%5Cz250mq0AaPwQDhH0qn7v5Tf41s7X2YJz7kokIL8B3HVtZmdb9efAJ4bN%5CDxdpd9Piq%5CEi2aO%2FfuAgT%2B3%2Fgvy2QV1SkRJfmV718ZRbiduDio7ylKa3HVkgx5ltKIVcVz7fblMXBO9Tt9KG0tSoVOKVLx9o%5CjmWSDVQPhG8ptzGucSyD%3A1538645096323; AUTH.YC.SBY=D165800876E25CE163D583DC44161F57AD41BEDF00296B4ECE99BBFFE12C3A877B017CEC4EBABAF8B8B9DA888650177059F0AED8C939BD27884E81707D206B25CB8296AAA3F132523EE0BD6C7408B85E771414F8D216E3E1D3FA97210DA0A3D940DB14F71A4774B983D2D4857FA0C5B5AFACBF7FC79B5ADC0799B98596ECBC61E22315EEB4E1FF7A6FCEEED65D6145989EC70D32C5D6F9DC1FD627BD72BEEF46F979EBA8EC3679601EA0C26BAF8426E96AA0B9878DC2376E5921B66DA6EAFD363DB6BC21853C10F831F579083397C06E924E6EB509C615C1E8751954FE4473993DD6FA2AA5B186584ED6263F4CA069FCA86B4E088AA6B72A30B9C17D556945D07912A15B8377E5FEC6CC1A7101A2FCA58B7FBD050827FFE9100DB99C0FA099B5C155678A77DBFA2D32F801900B6273B72A789C15384F45F0C9D9364FBFCB7164346609D4239F9CC8B1CED84A030740A10987C4768517A58E51C18ABC5B27A5F1174A4D727FEF7177974B679AF28CD57A3F0F431EC23ED80830DE046EC759C1566963FDD3E5EEBA79BA8C1D434F259296702F3CD5D3BE90593BF0DD926E41A8EED99201C79CA60ACF8E5944827F884A5674EF400E37E906502F16F307BB87B5296AC72EDCEC528DAFA98633FBC4AAD8BC307D241E15338693548C65824F4E4E824B68399569CF6F82F998D39C247FFB08CC95BDBFFE6EDEDD50C9E3BCF8F4B2F7FC95E655; AUTH.EXPIRED=MjAxODEwMDUwMTA4; SERVERID=aed0ea29d64e8dc43481ddfe295bddbb|1538644135|1538643896',
    # 'Cache-Control': 'no-cache',
}
data = {
    'AuditedEndTime': '',
    'AuditedStartTime': '',
    # 'DateLength': '29',
    'EndDate': '2018-09-30',
    'StartDate': '2018-09-01',
    'sorts[appliedTime]': 'desc',
    'sorts[zhiMaScore]': 'desc',
    'page': '1',
    'size': '50'
}
data_queue = Queue()
user_queue = Queue()
url_queue = Queue()

save_base_path = './'
user_dir = 'user'
url_dir = 'url'
cal_save_dir = 'tonghua_save'
tel_save_dir = 'tongxunlu_save'

url_interval = 0.5
url_wait = 5
cal_interval = 0.1
cal_wait = 0
tel_interval = 0.1
tel_wait = 0

cal_enabled = True
tel_enabled = True

start_date = datetime.date(2017, 11, 1)
end_date = datetime.date(2018, 10, 1)

user_count = 0
cal_count = 0
tel_count = 0

all_task = []
shutdown = False
user_over = False

NO_CAL_BTN = 'no_cal_btn'
NO_CAL = 'no_cal'
NO_TEL_BTN = 'no_tel_btn'
NO_TEL = 'no_tel'
fail_type_list = [NO_CAL_BTN, NO_CAL, NO_TEL_BTN, NO_TEL]

fail_list = {
    NO_CAL_BTN: [],
    NO_CAL: [],
    NO_TEL_BTN: [],
    NO_TEL: []
}


def get_excel_path(filename, dir):
    """根据文件名获取excel路径"""
    return os.path.join(save_base_path, dir, filename + ".xls")


def excel_exist(filename, dir):
    """根据文件名判断excel是否已经存在"""
    return os.path.exists(get_excel_path(filename, dir))


def save_excel(sheet_name, data, filename, dir):
    """保存excel"""
    save_path = get_excel_path(filename, dir)
    if os.path.exists(save_path):
        return
    # 创建工作薄
    workbook = Workbook(encoding='utf-8')
    sheet = workbook.add_sheet(sheet_name)
    for row in range(len(data)):
        row_data = data[row]
        for col in range(len(row_data)):
            sheet.write(row, col, row_data[col])
    # 保存本地文件
    workbook.save(save_path)


def user_count_add(data, count, valid_count, page_count=-1):
    """统计用户数量增加"""
    global user_count
    user_count += valid_count
    s1 = '%s申请一共%s页，每页%s条，' % (data.get('StartDate')[:7], page_count, data.get('size'))
    s2 = '开始获取%s第%s页用户列表，获取用户数量%d, 有效用户数量%s, 总有效用户数量%d' % \
         (data.get('StartDate')[:7], data.get('page'), count, valid_count, user_count)
    print((s1 + s2) if data.get('page') == '1' and page_count != -1 else s2)


def cal_count_add_one():
    """通话记录获取成功数量+1"""
    global cal_count
    global user_count
    cal_count += 1
    # print(url_queue.qsize())
    print('获取到通话记录的用户数/用户数: %d/%d' % (cal_count, user_count))
    check_cal_finish()


def tel_count_add_one():
    """通讯录获取成功数量+1"""
    global tel_count
    global user_count
    tel_count += 1
    print('获取到通讯录的用户数/用户数: %d/%d' % (tel_count, user_count))
    check_tel_finish()


def check_cal_finish():
    """检查通话记录是否已经获取结束，若结束打印获取到的数量以及为获取到的用户"""
    # print('cal:%d,%d' % (len(no_list['no_cal_btn']), len(no_list['no_cal'])))
    if len(fail_list[NO_CAL_BTN]) + len(fail_list[NO_CAL]) + cal_count < user_count:
        return
    if data_queue.qsize() or user_queue.qsize() or url_queue.qsize() or not user_over:
        return
    print('获取到通话记录的用户数/用户数: %d/%d' % (cal_count, user_count))
    if fail_list[NO_CAL_BTN]:
        print('没有详细数据清单按钮：')
        for i in fail_list[NO_CAL_BTN]:
            print(i)
    if fail_list[NO_CAL]:
        print('没有通话记录详情模块：')
        for i in fail_list[NO_CAL]:
            print(i)


def check_tel_finish():
    """检查通讯录是否已经获取结束，若结束打印获取到的数量以及为获取到的用户"""
    # print('tel:%d,%d' % (len(no_list['no_tel_btn']), len(no_list['no_tel'])))
    if len(fail_list[NO_TEL_BTN]) + len(fail_list[NO_TEL]) + tel_count < user_count:
        return
    if data_queue.qsize() or user_queue.qsize() or url_queue.qsize() or not user_over:
        return
    print('获取到通讯录的用户数/用户数: %d/%d' % (tel_count, user_count))
    if fail_list[NO_TEL_BTN]:
        print('没有手机通讯录按钮：')
        for i in fail_list[NO_TEL_BTN]:
            print(i)
    if fail_list[NO_TEL]:
        print('没有通讯录内容：')
        for i in fail_list[NO_TEL]:
            print(i)


def add_fail(type, text):
    """添加失败的信息"""
    if type not in fail_type_list:
        raise Exception('type param error')
    fail_list[type].append(text)
    check_cal_finish()
    check_tel_finish()


def read_user_list_from_excel(filename, data):
    """从excel中读取用户列表到user_queue, 过滤没有名字的用户"""
    workbook = open_workbook(get_excel_path(filename, user_dir))
    worksheet = workbook.sheets()[0]
    valid_count = 0
    for row_index in range(worksheet.nrows):
        name = worksheet.cell_value(row_index, 2)
        if not name:
            continue
        user = {}
        user['customerApplyFormData'] = worksheet.cell_value(row_index, 0)
        user['cellPhoneNumber'] = worksheet.cell_value(row_index, 1)
        user['name'] = worksheet.cell_value(row_index, 2)
        valid_count += 1
        user_queue.put(user)
    user_count_add(data, worksheet.nrows, valid_count)


def read_user_list_from_ajax_result(result, filename, data):
    """从ajax请求的结果数据中读取用户列表到user_queue并存入excel"""
    user_list = result.get('data')
    page_count = result.get('pageCount')
    # 将获取的用户放入user_queue，过滤没有名字的用户
    excel_data = []
    valid_count = 0
    for user in user_list:
        l = [user.get('customerApplyFormData'), user.get('cellPhoneNumber'), user.get('name')]
        excel_data.append(l)
        if user.get('name'):
            valid_count += 1
            user_queue.put(user)
    # 添加用户数量，提示数量增加
    user_count_add(data, len(user_list), valid_count, page_count)
    # 存入excel 2017-10-page1
    save_excel(filename, excel_data, filename, user_dir)


def crawl_user_list_from_data(data):
    """爬取用户列表，一次获取一页数据"""
    # 若用户excel文件存在，直接从文件获取，防止重复爬取消耗时间
    filename = '%s-page%s' % (data.get('StartDate')[:7], data.get('page'))
    if excel_exist(filename, user_dir) and data.get('page') != '1':
        # 从excel中读取用户列表到user_queue
        read_user_list_from_excel(filename, data)
        return 0
    response = requests.post(base_url + '/admin/applyform/list', data=data, headers=headers)
    if response.status_code != 200:
        # 获取用户列表失败 将data放回data_queue重试
        data_queue.put(copy.copy(data))
        print('获取%s第%s页用户列表，错误代码：%s' % (data.get('StartDate')[:7], data.get('page'), response.status_code))
        time.sleep(6)
        return 0
    result = json.loads(response.text)
    # 读取用户列表到user_queue并存入excel
    read_user_list_from_ajax_result(result, filename, data)
    time.sleep(6)
    return result.get('pageCount')


def crawl_user_list():
    """从data_queue取出data去爬取用户放入user_queue"""
    while True:
        if shutdown:
            return
        try:
            data = data_queue.get(block=False)  # 不阻塞
            page_count = crawl_user_list_from_data(data)
            # 若爬取的是第一页且页数大于1，继续爬取后面页数的数据
            if data.get('page') == '1' and page_count > 1:
                for i in range(2, page_count + 1):
                    if shutdown:
                        return
                    data['page'] = i
                    page_count = crawl_user_list_from_data(data)
                    # 防止爬取页码超过总页数
                    if page_count and data.get('page') > page_count:
                        break
        except queue.Empty:
            global user_over
            user_over = True
            print('用户列表爬取结束，总有效用户数量：%d' % user_count)
            return
        except:
            print(traceback.format_exc())
            data_queue.put(copy.copy(data))
            time.sleep(6)


def read_url_from_execl(tel, name, detail_url):
    """从excel中读取url列表到url_queue"""
    workbook = open_workbook(get_excel_path(tel + name, url_dir))
    worksheet = workbook.sheets()[0]
    for row_index in range(worksheet.nrows):
        dict_url = {}
        dict_url['tel'] = worksheet.cell_value(row_index, 0)
        dict_url['name'] = worksheet.cell_value(row_index, 1)
        dict_url['cal_url'] = worksheet.cell_value(row_index, 2)
        dict_url['tel_url'] = worksheet.cell_value(row_index, 3)
        if not dict_url['cal_url']:
            add_fail(NO_CAL_BTN, tel + name + ' ' + detail_url)
            print(name + ' 没有详细数据清单 ' + detail_url)
        if not dict_url['tel_url']:
            add_fail(NO_TEL_BTN, tel + name + ' ' + detail_url)
            print(name + ' 没有手机通讯录 ' + detail_url)
        if dict_url['cal_url'] or dict_url['tel_url']:
            url_queue.put(dict_url)


def read_url_from_response_text(response_text, tel, name, detail_url):
    """从页面响应文本中解析url放入url_queue并存入excel"""
    html = etree.HTML(response_text)
    cal_li = html.xpath("//a[contains(text(), '详细数据清单')]/..")
    tel_li = html.xpath("//a[contains(text(), '手机通讯录')]/..")
    cal_url = ''
    tel_url = ''
    if len(cal_li) != 0:
        cal_url = base_url + cal_li[0].get('data-url')
    else:
        # 没有详细数据清单按钮
        add_fail(NO_CAL_BTN, tel + name + ' ' + detail_url)
        print(name + ' 没有详细数据清单按钮 ' + detail_url)
    if len(tel_li) != 0:
        tel_url = base_url + tel_li[0].get('data-url')
    else:
        # 没有通讯录按钮
        add_fail(NO_TEL_BTN, tel + name + ' ' + detail_url)
        print(name + ' 没有手机通讯录按钮 ' + detail_url)
    if not cal_url and not tel_url:
        return
    url_queue.put({'tel': tel, 'name': name, 'cal_url': cal_url, 'tel_url': tel_url})
    # 存入excel 2017-10-page1
    d = [[tel, name, cal_url, tel_url]]
    save_excel(tel + name, d, tel + name, url_dir)


def crawl_url_from_user(user):
    """根据user的信息获取详情页url，爬取详情页中详细数据清单和手机通讯录的url"""
    try:
        name = user.get('name')
        tel = user.get('cellPhoneNumber')
        cafd = user.get('customerApplyFormData')
        cafd_encry = base64.b64encode(('["' + cafd + '"]').encode('utf-8'))
        detail_url = base_url + '/admin/applyform/detail/' + str(cafd_encry, 'utf-8')
        if not name:
            print('没有名字, ' + detail_url)
            return
        if excel_exist(tel + name, cal_save_dir) and excel_exist(tel + name, tel_save_dir):
            # 已经存在通话记录和通讯录文件则跳过
            print(tel + name + '已经获取过了')
            cal_count_add_one()
            tel_count_add_one()
            return
        # 若存在文件
        if excel_exist(tel + name, url_dir):
            # 从excel中读取url列表到url_queue
            read_url_from_execl(tel, name, detail_url)
            return
        response = requests.get(detail_url, headers=headers, timeout=5)
        # 获取详情页内容失败 将user放回user_queue重试
        if response.text == '请求过于频繁':
            print(name + ' 请求过于频繁 ' + detail_url)
            user_queue.put(user)
            # 过于频繁后等待几秒
            time.sleep(url_wait)
            return
        # 从页面响应文本中解析url放入url_queue
        read_url_from_response_text(response.text, tel, name, detail_url)
        time.sleep(url_interval)
    except:
        print(traceback.format_exc())
        user_queue.put(user)
        time.sleep(url_wait)


def crawl_url():
    """从user_queue取出用户，根据用户信息获取详情页url,爬取详情页中详细数据清单的url和手机通讯录的url, 放入url_queue"""
    while True:
        if shutdown:
            return
        try:
            user = user_queue.get()
            crawl_url_from_user(user)
        except:
            print(traceback.format_exc())


def crawl_cal(dict_url):
    """根据通话记录url获取通话记录，存入excel"""
    try:
        tel = dict_url.get('tel')
        name = dict_url.get('name')
        if excel_exist(tel + name, cal_save_dir):
            print(tel + name + ' 通话记录已经获取过了')
            cal_count_add_one()
            return
        url = dict_url.get('cal_url')
        print(name + '开始获取通话记录')
        response = requests.get(url, headers=headers, timeout=5)
        html = etree.HTML(response.text)
        div = html.xpath("//div[@class='error-number text-azure']")
        if div:
            print(name + '，通话记录详情500出错 ' + url)
            add_fail(NO_CAL, tel + name + ' ' + url)
            time.sleep(cal_wait)
            return
        table = html.xpath("//th[contains(text(), '通话记录详情')]/../..")
        if len(table) != 0:
            table = table[0]
        else:
            print(name + '，没有通话记录详情,' + url)
            add_fail(NO_CAL, tel + name + ' ' + url)
            time.sleep(cal_interval)
            return
        trs = table.xpath('.//tr')[1:]
        excel_data = []
        for tr in trs:
            tds = tr.xpath('.//td | .//th')
            excel_data.append([])
            for td in tds:
                excel_data[-1].append(td.text)
        # 保存文件
        save_excel(name, excel_data, tel + name, cal_save_dir)
        print(name + '结束获取通话记录')
        cal_count_add_one()
        time.sleep(cal_interval)
    except:
        print(traceback.format_exc())
        url_queue.put(dict_url)
        time.sleep(cal_wait)


def crawl_tel(dict_url):
    """根据通讯录url获取通讯录，存入excel"""
    try:
        tel = dict_url.get('tel')
        name = dict_url.get('name')
        if excel_exist(tel + name, tel_save_dir):
            print(tel + name + '，通讯录已经获取过了')
            tel_count_add_one()
            return
        url = dict_url.get('tel_url')
        print(name + '开始获取通讯录')
        response = requests.get(url, headers=headers, timeout=5)
        html = etree.HTML(response.text)
        lis = html.xpath("//div[@id='divAddressBook']//li")
        if not lis:
            print(name + '，通讯录为空,' + url)
            add_fail(NO_TEL, tel + name + ' ' + url)
            time.sleep(tel_interval)
            return
        excel_data = [[li.get('data-name'), li.get('data-phone')] for li in lis]
        # for li in lis:
        #     data_name = li.get('data-name')
        #     data_phone = li.get('data-phone')
        #     excel_data.append([data_name, data_phone])
        # 保存文件
        save_excel(name, excel_data, tel + name, tel_save_dir)
        tel_count_add_one()
        print(name + '结束获取通讯录')
        time.sleep(tel_interval)
    except:
        print(traceback.format_exc())
        url_queue.put(dict_url)
        time.sleep(tel_wait)


def crawl_cal_and_tel():
    """从url_queue取出两个url，爬虫网页中的通话记录和通讯录，存储在excel中"""
    while True:
        if shutdown:
            return
        dict_url = url_queue.get()
        global cal_enabled, tel_enabled
        if cal_enabled and dict_url.get('cal_url'):
            crawl_cal(dict_url)
        if tel_enabled and dict_url.get('tel_url'):
            crawl_tel(dict_url)


def generate_dates(start_date, end_date):
    """迭代date生成器，一次加一月"""
    num = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
    date = datetime.datetime(start_date.year, start_date.month, 1)
    yield date
    for i in range(num):
        days = calendar.monthrange(date.year, date.month)[1]
        date += datetime.timedelta(days=days)
        yield date


def create_dir():
    """创建存储excel用的4个文件夹"""
    dir_list = [
        os.path.join(save_base_path, user_dir),
        os.path.join(save_base_path, url_dir),
        os.path.join(save_base_path, cal_save_dir),
        os.path.join(save_base_path, tel_save_dir)
    ]
    for dir in dir_list:
        if not os.path.exists(dir):
            os.makedirs(dir)


def start(url, cookie, start, end, save_path2,
          url_interval2, url_wait2,
          cal_interval2, cal_wait2,
          tel_interval2, tel_wait2,
          cal_enabled2, tel_enabled2):
    """设置全局参数，遍历开始月份到结束月份，构造data放入data_queue，开始线程爬取"""
    global base_url, url_interval, url_wait, cal_interval, cal_wait, tel_interval, tel_wait, \
        cal_enabled, tel_enabled, start_date, end_date, headers, save_base_path, shutdown, user_over
    stop()
    base_url = url
    headers['cookie'] = cookie
    save_base_path = save_path2
    create_dir()
    url_interval = url_interval2
    url_wait = url_wait2
    cal_interval = cal_interval2
    cal_wait = cal_wait2
    tel_interval = tel_interval2
    tel_wait = tel_wait2
    cal_enabled = cal_enabled2
    tel_enabled = tel_enabled2
    start_date = start
    end_date = end
    for date in generate_dates(start_date, end_date):
        data['StartDate'] = str(date)
        days = calendar.monthrange(date.year, date.month)[1]
        data['EndDate'] = str(date + datetime.timedelta(days=days - 1))
        data_queue.put(copy.copy(data))
    shutdown = False
    user_over = False
    all_task.append(threading.Thread(target=crawl_user_list))
    all_task.append(threading.Thread(target=crawl_url))
    all_task.append(threading.Thread(target=crawl_cal_and_tel))
    for t in all_task:
        t.start()


def stop():
    """重置全局遍历，shutdown置为True"""
    global all_task, shutdown, user_over, data_queue, user_queue, url_queue, user_count, cal_count, tel_count
    shutdown = True
    user_over = False
    all_task = []
    data_queue = Queue()
    user_queue = Queue()
    url_queue = Queue()
    user_count = 0
    cal_count = 0
    tel_count = 0


if __name__ == '__main__':
    # 在data_queue依次放入 申请时间为start_date至end_date的data
    start_date = datetime.date(2017, 11, 1)
    end_date = datetime.date(2018, 10, 1)
    for date in generate_dates(start_date, end_date):
        data['StartDate'] = str(date)
        days = calendar.monthrange(date.year, date.month)[1]
        data['EndDate'] = str(date + datetime.timedelta(days=days - 1))
        data_queue.put(copy.copy(data))
    # 线程池，等待所有线程结束
    all_task = []
    executor = ThreadPoolExecutor(3)
    all_task.append(executor.submit(crawl_user_list))
    all_task.append(executor.submit(crawl_url))
    all_task.append(executor.submit(crawl_cal_and_tel))
    wait(all_task, return_when=ALL_COMPLETED)
