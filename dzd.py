# -*- coding: utf-8 -*-
from lxml import etree
from Queue import Queue
import MySQLdb, time, requests, sys, threading, sys

reload(sys)
sys.setdefaultencoding('utf-8')

data_queue = Queue()
page_queue = Queue()
exitFlag_Parser = False


url = "http://oms.sdzz.la:8080/admin/borrowBillCount/getBorrowBillPaidedList"
fromdata = {"pageNo": "1",
            "s_startDate":'2015-08-31',
            "s_endDate":'2017-06-19',
            "s_billStatus": "",
            "borrowType": "",
            "s_searchKey": "",
            "s_searchKeyType": "",
            "roleId": "",
            "adminId": "",
            "borrowCashFrom": ""}


# 获取有多少页
def page():
    global url, fromdata
    try:
        response = requests.post(url, data=fromdata)
        html = response.text
        select = etree.HTML(html)
        page = select.xpath("/html/body/span/a[3]/text()")[0]
        return page
    except Exception, e:
        print "请求页数失败", e


def start(data_queue,page_queue):
    global url
    while True:
        if page_queue.empty():
            break
        else:
            a = page_queue.get()
            print "开始爬取第%s页" % a
            f= open('page.txt','w+')
            f.write(str(a))
            f.close()
            fromdata = {"pageNo": str(a),
                        "s_startDate":'2015-08-31',
                        "s_endDate":'2017-06-19',
                        "s_billStatus": "",
                        "borrowType": "",
                        "s_searchKey": "",
                        "s_searchKeyType": "",
                        "roleId": "",
                        "adminId": "",
                        "borrowCashFrom": ""}
            timeout = 4
            while timeout > 0:
                timeout -= 1
                try:
                    response = requests.post(url, data=fromdata)
                    html = response.text
                    data_queue.put(html)
                    break
                except Exception, e:
                    print "请求第%s页失败" % str(a), e


# 解析每页html数据获得内容
def getdata(data_queue):
    data = {}
    print "开始解析"
    while not exitFlag_Parser:
        try:
            html = data_queue.get(timeout=120)

            sele = etree.HTML(html)

            shulist = sele.xpath("/html/body/table//tr")

            for i in shulist[1:]:
                data["xulie"] = i.xpath(".//td[1]/text()")[0]  # 序列

                data["Booked_time"] = i.xpath(".//td[2]/text()")[0]  # 入账时间

                data["zong_money"] = i.xpath(".//td[3]/text()")[0]  # 总金额

                data['cash'] = i.xpath(".//td[4]/text()")[0]  # 现金(不含砍头息)

                data['deduction'] = i.xpath(".//td[5]/text()")[0]  # 抵扣

                data['kan_tou_xi'] = i.xpath(".//td[6]/text()")[0]  # 砍头息

                data['yinghuan_money'] = i.xpath(".//td[7]/text()")[0]  # 应还金额

                data['overdue_days'] = i.xpath(".//td[8]/text()")[0]  # 逾期天数

                data['order_number'] = i.xpath(".//td[9]/text()")[0]  # 订单号

                data['name'] = i.xpath(".//td[10]/text()")[0]  # 姓名

                data['phone'] = i.xpath(".//td[11]/text()")[0]  # 电话

                data['Borrowing_time'] = i.xpath(".//td[12]/text()")[0]  # 借款时间

                data['Borrowing_amount'] = i.xpath(".//td[13]/text()")[0]  # 借款金额

                data['Arrive_time'] = i.xpath(".//td[14]/text()")[0]  # 到账时间

                data['yinghuan_time'] = i.xpath(".//td[15]/text()")[0]  # 应还时间

                data['expire_yinghuan'] = i.xpath(".//td[16]/text()")[0]  # 到期应还

                mysql(data)
        except Exception, e:
            print "Error",e
            break


def mysql(data):
    print "链接数据库"
    try:
        mysqlcli = MySQLdb.connect(host='120.92.74.189', user='root', passwd='Rf804dhu31vm6H9i4shg3j19kn65dQ',
                                   db='spider_data', port=3306, charset="utf8")
        a = data["phone"]
        print a

        sql1 = "select count(*) from statements_list1 where phone = " + a
        cur = mysqlcli.cursor()
        cur.execute(sql1)
        result = cur.fetchone()  # 没有数据返回0
        print result[0]
        if result[0] == 0:
            try:
                sql = "INSERT INTO statements_list1 (Booked_time,zong_money,cash,deduction,kan_tou_xi,yinghuan_money,overdue_days,order_number,name,phone,Borrowing_time,Borrowing_amount,Arrive_time,yinghuan_time,expire_yinghuan) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                items = [data['Booked_time'], data['zong_money'], data['cash'], data['deduction'], data['kan_tou_xi'],
                         data['yinghuan_money'], data['overdue_days'], data['order_number'], data['name'],
                         data['phone'], data['Borrowing_time'], data['Borrowing_amount'], data['Arrive_time'],
                         data['yinghuan_time'], data['expire_yinghuan']]
                cur = mysqlcli.cursor()
                a = cur.execute(sql, items)
                if a == 1:
                    print "插入成功"
                mysqlcli.commit()
                cur.close()
            except Exception, e:
                print "链接插入数据出现错误",e
        else:
            # sql = "update statements_list set zong_money='%s',cash='%s',deduction='%s',kan_tou_xi='%s',yinghuan_money='%s',order_number='%s',Borrowing_time='%s',Borrowing_amount='%s',Arrive_time='%s',yinghuan_time='%s',expire_yinghuan='%s',overdue_days='%s',Booked_time='%s' where phone='%s'" % (
            # data['zong_money'], data['cash'], data['deduction'], data['kan_tou_xi'], data['yinghuan_money'],
            # data['order_number'], data['Borrowing_time'], data['Borrowing_amount'], data['Arrive_time'],
            # data['yinghuan_time'], data['expire_yinghuan'], data['overdue_days'], data['Booked_time'], data['phone'])
            # cur.execute(sql)
            # mysqlcli.commit()
            # cur.close()
            print "重复数据"
    except Exception, e:
        print "数据库发生错误", e


def main():
    num = page()
    print num
    num = int(num)
    F = open('page.txt','r')
    a = F.read()
    F.close()
    if a =="":
        for i in range(1, num + 1):
            page_queue.put(i)
    else:
        for i in range(int(a),num+1):
            page_queue.put(i)
    crawlthreads = []

    # for crawl in range(3):
    thread = threading.Thread(target=start, args=(data_queue, page_queue,))
    thread.start()
        # crawlthreads.append(thread)
    parserthreads = []
    for parser in range(3):
        thread = threading.Thread(target=getdata, args=(data_queue,))
        thread.start()
        parserthreads.append(thread)
    # 等待页数队列清空
    while not page_queue.empty():
        pass
    # 等待所有线程完成
    # for t in crawlthreads:
    #     t.join()
    thread.join()
    while not data_queue.empty():
        pass

    global exitFlag_Parser
    exitFlag_Parser = True
    for t in parserthreads:
        t.join()
    print "主线程退出"


if __name__ == "__main__":
    main()






