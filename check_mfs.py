#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import struct
import time
import traceback
import sys
import smtplib
from email.mime.text import MIMEText
from daemon import Daemon

#MooseFS 主控服务器配置
masterhost = '127.0.0.1'
masterport = 9421
mastername = 'MooseFS'

#硬盘容量警告百分比
WARNING_HDD = 90

#重复项通知间隔时间,单位：秒
NOTIFY_INT = 3600

#监控检查间隔时间,单位：秒
SLEEP_TIME = 60;

#############
#要通知给谁，填写email地址
mailto_list=["@email.com"]
#####################
#设置SMTP服务器，用户名、口令以及邮箱的后缀
mail_host="smtp.163.com"
mail_user=""
mail_pass=""
mail_postfix="163.com"
######################

PROTO_BASE = 0

CLTOMA_CSERV_LIST = (PROTO_BASE+500)
MATOCL_CSERV_LIST = (PROTO_BASE+501)
CLTOMA_INFO = (PROTO_BASE+510)
MATOCL_INFO = (PROTO_BASE+511)
CLTOMA_MLOG_LIST = (PROTO_BASE+522)
MATOCL_MLOG_LIST = (PROTO_BASE+523)
CLTOCS_HDD_LIST_V2 = (PROTO_BASE+600)
CSTOCL_HDD_LIST_V2 = (PROTO_BASE+601)


acts = {}
def htmlentities(str):
	return str.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace("'",'&apos;').replace('"','&quot;')

def mysend(socket,msg):
	totalsent = 0
	while totalsent < len(msg):
		sent = socket.send(msg[totalsent:])
		if sent == 0:
			raise RuntimeError, "socket connection broken"
		totalsent = totalsent + sent

def myrecv(socket,leng):
	msg = ''
	while len(msg) < leng:
		chunk = socket.recv(leng-len(msg))
		if chunk == '':
			raise RuntimeError, "socket connection broken"
		msg = msg + chunk
	return msg

def send_mail(sub,content):
    '''
    sub:主题
    content:内容
    send_mail("sub","content")
    '''
    me=mail_user+"<"+mail_user+"@"+mail_postfix+">"
    msg = MIMEText(content)
    msg['Subject'] = sub
    msg['From'] = me
    msg['To'] = ";".join(mailto_list)
    try:
        s = smtplib.SMTP()
        s.connect(mail_host)
        s.login(mail_user,mail_pass)
        s.sendmail(me, mailto_list, msg.as_string())
        s.close()
        return True
    except Exception, e:
        print str(e)
        return False
		
def notify(act):
	if (acts.get(act, 0) + NOTIFY_INT < int(time.time())):
		acts[act] = int(time.time())
		return True
	else:
		return False
		
def del_act(act):
	if acts.has_key(act):
		del acts[act]
class MyDaemon(Daemon):
	def run(self):
		while True:		
			out = []
			# check version
			masterversion = (0,0,0)
			try:
				s = socket.socket()
				s.connect((masterhost,masterport))
				mysend(s,struct.pack(">LL",CLTOMA_INFO,0))
				header = myrecv(s,8)
				cmd,length = struct.unpack(">LL",header)
				data = myrecv(s,length)
				if cmd==MATOCL_INFO:
					if length==52:
						masterversion = (1,4,0)
					elif length==60:
						masterversion = (1,5,0)
					elif length==68 or length==76:
						masterversion = struct.unpack(">HBB",data[:4])
			except Exception:
				if notify("check_master"):
					send_mail("""Can't connect to MFS master (IP:%s ; PORT:%u)""" % (htmlentities(masterhost),masterport), "请求连接主控服务器失败")
				print """Can't connect to MFS master (IP:%s ; PORT:%u)""" % (htmlentities(masterhost),masterport)
				exit()

			if masterversion==(0,0,0):
				if notify("check_master"):
					send_mail("""Can't detect MFS master version (IP:%s ; PORT:%u)""" % (htmlentities(masterhost),masterport), "MFS主控服务器软件版本获取失败")
				print """Can't detect MFS master version"""
				exit()

			del_act("check_master");

			#检查挂载服务器	
			try:
				s = socket.socket()
				s.connect((masterhost,masterport))
				mysend(s,struct.pack(">LL",CLTOMA_CSERV_LIST,0))
				header = myrecv(s,8)
				cmd,length = struct.unpack(">LL",header)
				if cmd==MATOCL_CSERV_LIST and masterversion>=(1,5,13) and (length%54)==0:
					del_act("check_mount")
					data = myrecv(s,length)
					n = length/54
					for i in xrange(n):
						d = data[i*54:(i+1)*54]
						disconnected,v1,v2,v3,ip1,ip2,ip3,ip4,port,used,total,chunks,tdused,tdtotal,tdchunks,errcnt = struct.unpack(">BBBBBBBBHQQLQQLL",d)
						strip = "%u.%u.%u.%u" % (ip1,ip2,ip3,ip4)
						try:
							host = (socket.gethostbyaddr(strip))[0]
						except Exception:
							host = "(unresolved)"

						if disconnected==1:
							if notify(strip):
								out.append("""%s:%s%s 链接断开""" % (strip,port,host))
						else:
							if (total>0):
								if (int((used*100)/total) >= WARNING_HDD):
									if notify(strip):
										out.append("""%s:%s%s 硬盘容量超过%u%%""" % (strip,port,host,WARNING_HDD))
								else:
									del_act(strip)
							elif notify(strip):
								out.append("""%s:%s%s 硬盘容量未知""" % (strip,port,host))			
				elif cmd==MATOCL_CSERV_LIST and masterversion<(1,5,13) and (length%50)==0:
					del_act("check_mount")
					data = myrecv(s,length)
					n = length/50
					for i in xrange(n):
						d = data[i*50:(i+1)*50]
						ip1,ip2,ip3,ip4,port,used,total,chunks,tdused,tdtotal,tdchunks,errcnt = struct.unpack(">BBBBHQQLQQLL",d)
						strip = "%u.%u.%u.%u" % (ip1,ip2,ip3,ip4)
						try:
							host = (socket.gethostbyaddr(strip))[0]
						except Exception:
							host = "(unresolved)"

						if (total>0):
							if (int((used*100)/total) >= WARNING_HDD):
								if notify(strip):
									out.append("""%s:%s%s 硬盘容量超过%u%%""" % (strip,port,host,WARNING_HDD))
							else:
								del_act(strip)
						elif notify(strip):
							out.append("""%s:%s%s 硬盘容量未知""" % (strip,port,host))
				elif notify("check_mount"):
					out.append("""检查挂载服务器失败cmd""")
				s.close()
			except Exception:
				if notify("check_mount"):
					out.append("""检查挂载服务器失败link""")
				traceback.print_exc(file=sys.stdout)

			#检查日志服务器	
			if masterversion>=(1,6,5):
				try:
					s = socket.socket()
					s.connect((masterhost,masterport))
					mysend(s,struct.pack(">LL",CLTOMA_MLOG_LIST,0))
					header = myrecv(s,8)
					cmd,length = struct.unpack(">LL",header)
					if cmd==MATOCL_MLOG_LIST and (length%8)==0:
						data = myrecv(s,length)
						n = length/8
						if n==0 and notify("check_log"):
							out.append("""未发现日志服务器""")
						if n > 0:
							del_act("check_log")
					elif notify("check_log"):
						out.append("""检查日志服务器失败cmd""")
					
					s.close()
				except Exception:
					if notify("check_log"):
						out.append("""检查日志服务器失败""")
					traceback.print_exc(file=sys.stdout)

			#检查挂载硬盘
			try:
				# get cs list
				hostlist = []
				s = socket.socket()
				s.connect((masterhost,masterport))
				mysend(s,struct.pack(">LL",CLTOMA_CSERV_LIST,0))
				header = myrecv(s,8)
				cmd,length = struct.unpack(">LL",header)
				if cmd==MATOCL_CSERV_LIST and masterversion>=(1,5,13) and (length%54)==0:
					del_act("check_hdd")
					data = myrecv(s,length)
					n = length/54
					servers = []
					for i in xrange(n):
						d = data[i*54:(i+1)*54]
						disconnected,v1,v2,v3,ip1,ip2,ip3,ip4,port,used,total,chunks,tdused,tdtotal,tdchunks,errcnt = struct.unpack(">BBBBBBBBHQQLQQLL",d)
						if disconnected==0:
							hostlist.append((v1,v2,v3,ip1,ip2,ip3,ip4,port))
						else:
							hostip = "%u.%u.%u.%u" % (ip1,ip2,ip3,ip4)
							if notify(hostip):
								out.append("""%s:%s%s 链接断开hdd""" % (hostip,port))
				elif cmd==MATOCL_CSERV_LIST and masterversion<(1,5,13) and (length%50)==0:
					del_act("check_hdd")
					data = myrecv(s,length)
					n = length/50
					servers = []
					for i in xrange(n):
						d = data[i*50:(i+1)*50]
						ip1,ip2,ip3,ip4,port,used,total,chunks,tdused,tdtotal,tdchunks,errcnt = struct.unpack(">BBBBHQQLQQLL",d)
						hostlist.append((1,5,0,ip1,ip2,ip3,ip4,port))
				elif notify('check_hdd'):
					out.append("""检查挂载硬盘返回异常""")
				s.close()
				
				hdd = []
				for v1,v2,v3,ip1,ip2,ip3,ip4,port in hostlist:
					hostip = "%u.%u.%u.%u" % (ip1,ip2,ip3,ip4)
					try:
						hoststr = (socket.gethostbyaddr(hostip))[0]
					except Exception:
						hoststr = "(unresolved)"
					if port>0:
						if (v1,v2,v3)<=(1,6,8):
							s = socket.socket()
							s.connect((hostip,port))
							mysend(s,struct.pack(">LL",CLTOCS_HDD_LIST_V1,0))
							header = myrecv(s,8)
							cmd,length = struct.unpack(">LL",header)
							if cmd==CSTOCL_HDD_LIST_V1:
								del_act(hostip);
								data = myrecv(s,length)
								while length>0:
									plen = ord(data[0])
									path = "%s:%u:%s" % (hostip,port,data[1:plen+1])
									flags,errchunkid,errtime,used,total,chunkscnt = struct.unpack(">BQLQQL",data[plen+1:plen+34])
									length -= plen+34
									data = data[plen+34:]

									hdd.append((path,flags,errchunkid,errtime,used,total,chunkscnt,0,0,0,0,0,0,0,0,0,0,0,0))
							elif notify(hostip):
								out.append("""%s:%s%s 返回异常cms""" % (hostip,port))
							s.close()
						else:
							s = socket.socket()
							s.connect((hostip,port))
							mysend(s,struct.pack(">LL",CLTOCS_HDD_LIST_V2,0))
							header = myrecv(s,8)
							cmd,length = struct.unpack(">LL",header)
							if cmd==CSTOCL_HDD_LIST_V2:
								del_act(hostip)
								data = myrecv(s,length)
								while length>0:
									entrysize = struct.unpack(">H",data[:2])[0]
									entry = data[2:2+entrysize]
									data = data[2+entrysize:]
									length -= 2+entrysize;

									plen = ord(entry[0])
									path = "%s:%u:%s" % (hostip,port,entry[1:plen+1])
									flags,errchunkid,errtime,used,total,chunkscnt = struct.unpack(">BQLQQL",entry[plen+1:plen+34])

									hdd.append((path,flags,errchunkid,errtime,used,total,chunkscnt))
							elif notify(hostip):
								out.append("""%s:%s%s 返回异常cms""" % (hostip,port))
							s.close()
					elif notify(hostip):
						out.append("""%s:%s%s 链接断开hdd""" % (hostip,port))
				if len(hdd)>0:
					for path,flags,errchunkid,errtime,used,total,chunkscnt in hdd:
						if flags==1:
							if masterversion>=(1,6,10):
								status = 'marked for removal'
							else:
								status = 'to be empty'
						elif flags==2:
							status = 'damaged'
						elif flags==3:
							if masterversion>=(1,6,10):
								status = 'damaged, marked for removal'
							else:
								status = 'damaged, to be empty'
						elif flags==4 or flags==6:
							status = 'scanning'
						elif flags==5 or flags==7:
							status = 'marked for removal, scanning'
						else:
							status = 'ok'
						if errtime==0 and errchunkid==0:
							lerror = 'no'
						else:
							errtimetuple = time.localtime(errtime)
							lerror = '%s on chunk: %u' % (time.strftime("%Y-%m-%d %H:%M:%S",errtimetuple),errchunkid)
							
						if status != 'ok' or lerror != 'no':
							if notify(path):
								out.append("""IP path:%s  chunks:%u  last error:%s   status:%s""" % (path,chunkscnt,lerror,status))
						else:
							del_act("check_hdd")
							del_act(path)
				elif notify("check_hdd"):
					out.append("""未发现挂载硬盘""")
			except Exception:
				if notify("check_hdd"):
					out.append("""检查挂载硬盘异常""")
				traceback.print_exc(file=sys.stdout)
				
			if len(out) >0:
				content = "\n".join(out)
				print content
				send_mail("MFS监控报告", content)
			time.sleep(SLEEP_TIME)

if __name__ == "__main__":
	daemon = MyDaemon('/tmp/check_mfs.pid')
	if len(sys.argv) == 2:
		if 'start' == sys.argv[1]:
			daemon.start()
		elif 'stop' == sys.argv[1]:
			daemon.stop()
		elif 'restart' == sys.argv[1]:
			daemon.restart()
		else:
			print "Unknown command"
			sys.exit(2)
		sys.exit(0)
	else:
		print "usage: %s start|stop|restart" % sys.argv[0]
		sys.exit(2)			
