#-*- coding: utf-8 -*-
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email import encoders
from email.mime.base import MIMEBase
from email.utils import parseaddr,formataddr
import MySQLdb,time,csv
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
d = time.strftime('%Y-%m-%d',time.localtime(time.time()-86400))
c = d+".csv"
def mysql(c):


	a = d+"%"
	try:
		mysqlcli = MySQLdb.connect(host='120.92.74.189', user='root', passwd='Rf804dhu31vm6H9i4shg3j19kn65dQ',
								   db='spider_data', port=3306, charset="utf8")
		sql1 = "select overdue_days,name,phone,Booked_time from statements_list where Booked_time like '%s'"%a
		cur = mysqlcli.cursor()
		cur.execute(sql1)
		result = cur.fetchall()#查询出所有的数据
		cur.close()
		f = open(c,"wb")
		writer = csv.writer(f)
		writer.writerow(["姓名","手机号","逾期天数"])
		f.close()
		for res in result:#循环所有数据
			tianshu = res[0]
			try:
				#链接线上数据库
				mysqlcli2 = MySQLdb.connect(host='rm-bp16359eo425u37b8o.mysql.rds.aliyuncs.com', user='crawler', passwd='1234Qwer',
										   db='crawler', port=3306, charset="utf8")
				sql2 = "select count(*) from t_black_list where id = '%s'"%res[2]

				cur2 = mysqlcli2.cursor()
				#先查询黑名单是否有这条数据，如果有返回一个二维元组，里面的元组第一个是条数
				cur2.execute(sql2)
				mysqlcli2.commit()
				result = cur2.fetchone()#有数据result[0][0] ==1
				# print result[0][0]

			except Exception,e:
				mysqlcli2.rollback()
				print u"数据库发生错误",e
			if result[0]==1:#如果黑名单有数据等于1，不进行下面操作，跳过本次循环

				sql = "update t_black_list set timeout_day='%s' where id = '%s'"%(res[0],res[2])
				cur2.execute(sql)
				mysqlcli2.commit()
				print "数据存在黑名单更新数据"
				continue

			#如果黑名单没有这条数据，查询灰名单
			else:
				sql = "select count(*),mobile from t_grey_list where id = '%s'"%res[2]
				try:
					cur2.execute(sql)
					mysqlcli2.commit()
					result = cur2.fetchone()

					#如果灰名单有数据，判断爬的数据逾期天数是否大于15天，如果大于15天删除灰名单中的原始数据，将数据插入黑名单
					if result[0] == 1:
						if res[0]>15:
							print "逾期天数大于15天加入黑名单"
							try:
								print "删除灰名单数据"
								sql = "delete from t_grey_list where id = '%s'"%res[2]
								cur2.execute(sql)
								mysqlcli2.commit()
								crmysql(tianshu,res,cur2,mysqlcli2,c)
							except Exception,e:
								mysqlcli2.rollback()
								print u"数据库发生错误", e
						#如果爬的数据不大于15天，可以直接跳出循环，进行下次循环
						else:
							sql = "update t_grey_list set timeout_day='%s' where id = '%s'"%(res[0],res[2])
							cur2.execute(sql)
							mysqlcli2.commit()
							print "数据存在灰名单对数据进行更新"
							continue

						print result
					#如果灰名单中没有查到数据，查询白名单
					else:
						print "查询白名单"
						try:

							sql = "select count(*),mobile from t_white_list where id = '%s'"%res[2]
							cur2.execute(sql)
							mysqlcli2.commit()
							result = cur2.fetchone()
							#如果在白名单中查到数据，查看爬取数据天数，如果大于3天，删除白名单中信息，进行分表插入，如小于3天跳出循环
							if result[0] ==1:
								if res[0]<=3:
									sql = "update t_white_list set timeout_day='%s' where id = '%s'"%(res[0],res[2])
									cur2.execute(sql)
									mysqlcli2.commit()
									print "白名单已存在更新数据"
									continue
								#逾期天数大于3天，先删除白名单数据，在进行分表
								else:
									sql = "delete from t_white_list where id = '%s'" % res[2]
									cur2.execute(sql)
									mysqlcli2.commit()
									crmysql(tianshu,res,cur2,mysqlcli2,c)

							#如果没有，还是进行分表插入
							else:
								crmysql(tianshu,res,cur2,mysqlcli2,c)
						except Exception, e:
							mysqlcli2.rollback()
							print u"数据库发生错误",e
				except Exception,e:
					mysqlcli2.rollback()
					print u"数据库发生错误",e
	except Exception,e:
		mysqlcli.rollback()
		print u"数据库发生错误", e
def crmysql(tianshu,res,cur2,mysqlcli2,c):
	if tianshu<=3:#白名单
		try:
			sql = "INSERT INTO t_white_list (id,user_name,mobile,timeout_day) VALUES (%s,%s,%s,%s)"
			item = [res[2],res[1],res[2],res[0]]

			a = cur2.execute(sql,item)
			if a==1:
				print u"插入白名单成功"
			mysqlcli2.commit()
			f = open(c,"ab")
			writer = csv.writer(f)

			writer.writerow([res[1],res[2],res[0]])
			f.close()

		except Exception,e:
			mysqlcli2.rollback()
			print u"数据库发生错误",e

	elif tianshu<15 and tianshu>3:#灰名单
		try:
			sql = "INSERT INTO t_grey_list (id,user_name,mobile,timeout_day) VALUES (%s,%s,%s,%s)"
			item = [res[2],res[1],res[2],res[0]]

			a=cur2.execute(sql,item)
			if a==1:
				print u"插入灰名单成功"
			mysqlcli2.commit()
		except Exception,e:
			mysqlcli2.rollback()
			print "数据库发生错误",e
	elif tianshu>=15:#黑名单
		try:
			sql = "INSERT INTO t_black_list (id,user_name,mobile,timeout_day) VALUES (%s,%s,%s,%s)"
			item = [res[2],res[1],res[2],res[0]]

			a = cur2.execute(sql,item)
			if a==1:
				print u"插入黑名单成功"
			mysqlcli2.commit()
		except Exception,e:
			mysqlcli2.rollback()
			print "数据库发生错误",e
	cur2.close()

# cur.close()
# cur2.close()
# sendMail(' 请查收！', 'test.csv')
	# except Exception,e:
		# print u"数据库发生错误",e
#格式化邮件地址
def formatAddr(s):
	name, addr = parseaddr(s)
	return formataddr((Header(name, 'utf-8').encode(), addr))

def sendMail(body, attachment):
	smtp_server = 'smtp.qq.com'
	from_mail = '453712212@qq.com'#发信人账号
	mail_pass = 'dbcfigpftsrxbggh'
	to_mail = "wjh@aixinqianbao.com.cn"#收信人账号"wjh@aixinqianbao.com.cn"
	# 构造一个MIMEMultipart对象代表邮件本身
	msg = MIMEMultipart()
	# Header对中文进行转码
	msg['From'] = formatAddr('殷俊 <%s>' % from_mail).encode()#发送人姓名，和邮箱
	msg['To'] = to_mail#收信人邮箱
	msg['Subject'] = Header('你好', 'utf-8').encode()#标题
	# plain代表纯文本
	msg.attach(MIMEText(body, 'plain', 'utf-8'))#正文内容以文本的
	# 二进制方式模式文件
	with open(attachment, 'rb') as f:
		# MIMEBase表示附件的对象
		mime = MIMEBase('text', 'csv', filename=attachment)
		# filename是显示附件名字
		mime.add_header('Content-Disposition', 'attachment', filename=attachment)
		# 获取附件内容
		mime.set_payload(f.read())
		encoders.encode_base64(mime)
		# 作为附件添加到邮件
		msg.attach(mime)
	try:
		s = smtplib.SMTP_SSL(smtp_server,465)#465是SMTP的
		s.login(from_mail, mail_pass)
		s.sendmail(from_mail, to_mail, msg.as_string())  # as_string()把MIMEText对象变成str
		s.quit()
	except smtplib.SMTPException as e:
		print "Error: %s" % e

if __name__=="__main__":
	mysql(c)
	sendMail(' 请查收！',c)
