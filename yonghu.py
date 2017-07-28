# -*- coding: utf-8 -*-
import requests,Queue,time,re,threading,MySQLdb
from lxml import etree
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
htmlqueue = Queue.Queue()
id_queue = Queue.Queue()
url = "http://oms.sdzz.la:8080/admin/user/getUserInfoList"
d = time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400))
exitFlag_Parser = False
apply = False
#获得首页
def shouye(htmlqueue,d,url):

    formdata = {'startDate':d,
                'endDate':d,
                'mobile':"",
                'channel':"",
                'pageNo':"1",
                'pageSize':"500",
                'lastUserId':""}
    response = requests.post(url,data=formdata)
    html = response.text
    print "获得首页"
    htmlqueue.put(html)
    return html
#获得首页最后一个ID，循环发送post数据获得下一页数据
def shouye_id(html,htmlqueue,d,url):
    dom = etree.HTML(html)
    id = dom.xpath("/html/body//table//tr[501]/td[1]/text()")[0]
    id = int(id)

    while True:
        try:
            formdata = {'startDate':d,
                        'endDate':d,
                        'mobile':"",
                        'channel':"",
                        'pageNo':"1",
                        'pageSize':"500",
                        'lastUserId':str(id)}

            print "发送请求得到下一页"
            response = requests.post(url,data = formdata)

            html = response.text
            a = len(html)
            if a<21150:#页面长度小于21136说明页面为空，不在发送请求
                break
            #print html,id
            htmlqueue.put(html)
            id-=500
        except Exception,e:
            print u"发送请求失败",e
def jiexi(htmlqueue,id_queue):

    data ={}
    while not exitFlag_Parser:

        try:
            html = htmlqueue.get(timeout=60)
            print "正在进行解析"
            dom = etree.HTML(html)
            userlist = dom.xpath("/html/body//table//tr")
            for i in userlist[2:]:
                #用户id
                id = i.xpath(".//td[1]/text()")[0]

                data['id'] = id
                #得到手机号，姓名
                pmlist = i.xpath(".//td[2]/text()")[0]
                list_phone = pmlist.split('/')

                if len(list_phone)==0:
                    data['phone'] = ""
                    data['name'] = ""
                elif len(list_phone) ==1:
                    data['phone'] = list_phone[0]
                    data['name'] = ""
                else:
                    data['phone'] = list_phone[0]
                    data['name'] = list_phone[1]

                #身份证，银行卡号
                list_shenfen = i.xpath(".//td[3]/text()")

                if len(list_shenfen)==0:
                    data['id_card'] = ""
                    data['bank_number'] = ""
                elif len(list_shenfen)==1:
                    data['id_card'] = list_shenfen[0]
                    data['bank_number'] = ""
                else:
                    data['id_card'] = list_shenfen[0]
                    data['bank_number'] = list_shenfen[1]

                #信息来源
                source = i.xpath(".//td[4]/text()")
                if source==[]:
                    data['source'] = ""
                else:
                    data['source'] = source[0]

                #app版本
                app_version = i.xpath(".//td[5]/text()")
                if app_version==[]:
                    data['app_version']=""
                else:
                    data['app_version'] = app_version[0]

                #注册时间
                zhuce_time =i.xpath(".//td[6]/text()")
                if zhuce_time ==[]:
                    data['zhuce_time'] =  ""
                else:
                    data['zhuce_time'] = zhuce_time[0]

                #申请数
                sq_number = i.xpath(".//td[7]//a/text()")[0]
                data['sq_number'] = sq_number
                if int(data['sq_number']) > 0:

                    id_queue.put(data['id'])

                #申请成功数
                data['cg_number'] = i.xpath(".//td[8]/text()")[0]

                #标签
                label = i.xpath(".//td[9]/text()")
                if label ==[]:
                    data['label'] = ""
                else:
                    data['label'] = label[0]
                mysql(data)
        except Exception,e:
            print u"错误",e
            break
def shenqing(id_queue):
    print '获得用户申请信息'
    while not apply:
        try:
            id = id_queue.get(timeout=60)

            url = "http://oms.sdzz.la:8080/admin/borrow/getBorrowHisList?userId="+id

            response = requests.get(url)
            html = response.text

            DOM = etree.HTML(html)
            res = DOM.xpath('/html/body//table//tr')

            for i in res[1:]:
                shenqing = {}
                shenqing['id'] = id
                #申请时间
                shenqing['sq_time'] = i.xpath('.//td[2]/text()')[0]

                #订单号
                order_number = i.xpath('.//td[3]/text()')
                if order_number ==[]:
                    shenqing['order_number'] = ""
                else:
                    shenqing['order_number'] = order_number[0]
                #申请产品
                sq_product = i.xpath('.//td[4]/text()')
                if sq_product ==[]:
                    shenqing['sq_product'] =''
                else:
                    shenqing['sq_product'] = sq_product[0]
                #结果
                jieguo = i.xpath('.//td[5]/font/text()')
                if jieguo ==[]:
                    shenqing['jieguo'] =""
                else:
                    shenqing['jieguo'] = jieguo[0]

                #逾期天数
                overdue_days = i.xpath('.//td[6]/text()')
                if overdue_days ==[]:
                    shenqing['overdue_days'] = ""
                else:
                    shenqing['overdue_days'] = overdue_days[0]
                #逾期情况
                overdue_situation = i.xpath('.//td[7]/text()')
                if overdue_situation ==[]:
                    shenqing['overdue_situation'] = ""
                else:
                    shenqing['overdue_situation'] = overdue_situation[0]
                #备注
                beizhu = i.xpath('.//td[8]/text()')
                if beizhu ==[]:
                    shenqing['beizhu'] =""
                else:
                    shenqing['beizhu'] = beizhu[0]
                mysql_shenqing(shenqing)
        except Exception,e:
            print 'shenqing',e
            continue
        # mysql_shenqing(shenqing)
def mysql_shenqing(shenqing):
    try:
        mysqlcli1 = MySQLdb.connect(host='120.92.74.189', user='root', passwd='Rf804dhu31vm6H9i4shg3j19kn65dQ',
                                   db='spider_data', port=3306, charset="utf8")
        cur = mysqlcli1.cursor()
        sql = "INSERT INTO user_application (p_user_id,Application_time,order_number,product,result,overdue_days,overdue_situation,note) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        items = [shenqing['id'],shenqing['sq_time'],shenqing['order_number'],shenqing['sq_product'],shenqing['jieguo'],shenqing['overdue_days'],shenqing['overdue_situation'],shenqing['beizhu']]
        a = cur.execute(sql, items)
        if a == 1:
            print "插入成功"
        mysqlcli1.commit()
        cur.close()
    except Exception,e:
        print 'Error',e
        mysqlcli1.rollback()
def mysql(data):
    print "链接数据库"
    try:
        mysqlcli=MySQLdb.connect(host='120.92.74.189',user ='root',passwd='Rf804dhu31vm6H9i4shg3j19kn65dQ',db='spider_data',port= 3306,charset= "utf8")
        a = data["phone"]
        b = int(a)
        s = b%3
        name = "user_list"+str(s)
        sql1 = "select count(*) from "+name+" where phone = "+ a
        cur = mysqlcli.cursor()
        cur.execute(sql1)
        result=cur.fetchone() #没有数据返回0
        print result[0]
        # print result[0]
        if result[0] == 0:
            try:
                sql = "INSERT INTO "+name+" (user_id,name,phone,Id_card,bank_number,source,app_version,Registration_time,Application_number,successful_number,label) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                items = [data['id'],data['name'],data['phone'],data['id_card'],data['bank_number'],data['source'],data['app_version'],data['zhuce_time'],data['sq_number'],data['cg_number'],data['label']]

                cur = mysqlcli.cursor()
                a = cur.execute(sql,items)
                if a==1:
                    print "插入成功"
                mysqlcli.commit()
                cur.close()
            except Exception,e:
                print "链接插入数据出现错误",e
                mysqlcli.rollback()
        else:
            print "重复数据"
    except Exception,e:
        print "数据库发生错误",e
        mysqlcli.rollback()

if __name__=="__main__":
    html = shouye(htmlqueue,d,url)
    #发送请求
    thread = threading.Thread(target=shouye_id, args=(html,htmlqueue,d,url,))
    thread.start()
    #解析线程
    parserthreads = []
    for parser in range(3):
        thread = threading.Thread(target=jiexi, args=(htmlqueue,id_queue,))
        thread.start()
        parserthreads.append(thread)
    #申请记录方法线程
    shenqingthreads = []
    for sq in range(3):
        thread = threading.Thread(target=shenqing,args=(id_queue,))
        thread.start()
        shenqingthreads.append(thread)
    #等待请求线程完成
    thread.join()
    #请求结束后，查看htmlqueue是否为空，不为空一直等待
    while not htmlqueue.empty():
         pass
    #为空后结束解析线程的循环
    exitFlag_Parser = True
    #等待解析线程结束
    for i in parserthreads:
        i.join()
    while not id_queue.empty():
        pass

    apply = True
    for i in shenqingthreads:
        i.join()
    print "主线程退出"