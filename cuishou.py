# -*- coding: utf-8 -*-
import requests
from lxml import etree
import time,sys,MySQLdb
reload(sys)
sys.setdefaultencoding('utf-8')
def request_data():
    url = "http://oms.sdzz.la:8080/admin/overduecollection/getYuQiBorrowUserList"
    response  = requests.get(url)
    html = response.text
    print html
    selector = etree.HTML(html)
    overduelist = selector.xpath("//body/div//table//tr")


    for list in overduelist[2:]:
        data = {}
        # 姓名
        data["name"] = list.xpath(".//td[2]/text()")[0]

         #手机号
        data["phone"] = list.xpath(".//td[3]/text()")[0]

        #身份证
        data["Id_card"] = list.xpath(".//td[4]/text()")[0]

        #借款本金
        data["borrow_money"] = list.xpath(".//td[5]/text()")[0]

        #应还本金
        data["yinghuan_money"] = list.xpath(".//td[6]/text()")[0]

        #罚息金额
        data["faxi_money"] = list.xpath(".//td[7]/text()")[0]

        #以还金额
        data["yihuan_money"] = list.xpath(".//td[8]/text()")[0]

        #逾期笔数
        data["overdue_number"]= list.xpath(".//td[9]/a/text()")[0]

        #当前逾期时长
        data["overdue_length"] = list.xpath(".//td[10]/text()")[0]

        #剩余未还总额
        data["weihuan_money"] = list.xpath(".//td[11]/text()")[0]

        #催收员

        odv = list.xpath(".//td[12]/text()")
        if odv ==[]:
            data["odv"]=""
        else:
            data["odv"] = odv[0]
        #预期类型
        data["overdue_type"] = list.xpath(".//td[13]/text()")[0].strip()

        mysql_overdue(data)
def mysql_overdue(data):
    print "链接数据库"
    try:
        mysqlcli = MySQLdb.connect(host='120.92.74.189', user='root', passwd='Rf804dhu31vm6H9i4shg3j19kn65dQ',
                                   db='spider_data', port=3306, charset="utf8")
        a = data["phone"]
        print a
        sql1 = "select count(*) from overdue_list where phone_number = " + a
        cur = mysqlcli.cursor()
        cur.execute(sql1)
        result = cur.fetchone()
        print result[0]
        if result[0] == 0:
            try:
                sql = "INSERT INTO overdue_list (name,Phone_number,identity,borrow,yinghuan_borrow,penalty_xi,separated_money,overdue,overdue_duration,weihuan,odv,overdue_type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                items = [data["name"],data["phone"],data["Id_card"],data["borrow_money"],data["yinghuan_money"],data["faxi_money"],data["yihuan_money"],data["overdue_number"],data["overdue_length"],data["weihuan_money"],data["odv"],data["overdue_type"]]
                cur = mysqlcli.cursor()
                a = cur.execute(sql, items)
                if a == 1:
                    print "插入成功"
                mysqlcli.commit()
                cur.close()
            except Exception, e:
                print "链接插入数据出现错误", e
        else:
            print "重复数据进行下次循环"
    except Exception, e:
        print "数据库发生错误",e

if __name__=="__main__":
    request_data()