# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests,pymongo

client = pymongo.MongoClient('localhost',27017)
finance3 = client['finance']
info3 = finance3['info2']


i = 0

url = "http://oms.sdzz.la:8080/admin/borrowBillCount/getBorrowBillPaidedList"
public_headers = {
    "User-Agent": "User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36",
}


session = requests.session()
while i <124:
    i += 1
    params = {"pageNo": i, "s_startDate": "2017-06-17", "s_endDate":"2017-06-17","s_billStatus":"","borrowType":"","s_searchKey":"","s_searchKeyType":"","roleId":"","adminId":"","borrowCashFrom":""}
    index_page = session.request("POST", url, headers = public_headers, data=params)
    soup = BeautifulSoup(index_page.text,'lxml')
   # print(soup.select('body > table > tr > td')[0:16],len(soup.select('body > table > tr > td')))


    for k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11,k12,k13,k14,k15,k16 in zip(soup.select('body > table > tr > td')[16::16],soup.select('body > table > tr > td')[17::16],soup.select('body > table > tr > td')[18::16],soup.select('body > table > tr > td')[19::16],soup.select('body > table > tr > td')[20::16],soup.select('body > table > tr > td')[21::16],soup.select('body > table > tr > td')[22::16],soup.select('body > table > tr > td')[23::16],soup.select('body > table > tr > td')[24::16],soup.select('body > table > tr > td')[25::16],soup.select('body > table > tr > td')[26::16],soup.select('body > table > tr > td')[27::16],soup.select('body > table > tr > td')[28::16],soup.select('body > table > tr > td')[29::16],soup.select('body > table > tr > td')[30::16],soup.select('body > table > tr > td')[31::16]):
        data = {
            '序号':k1.get_text(),
            '入账时间':k2.get_text(),
            '总金额':k3.get_text(),
            '现金(不含砍头息)':k4.get_text(),
            '抵扣':k5.get_text(),
            '砍头息':k6.get_text(),
            '应还金额':k7.get_text(),
            '逾期天数':k8.get_text(),
            '订单号':k9.get_text(),
            '姓名':k10.get_text(),
            '电话':k11.get_text(),
            '借款时间':k12.get_text(),
            '借款金额':k13.get_text(),
            '到账时间':k14.get_text(),
            '应还时间':k15.get_text(),
            '到期应还':k16.get_text()
        }
        info3.insert_one(data)
