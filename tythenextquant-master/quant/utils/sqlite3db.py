# -*- coding:utf-8 -*-

"""
sqlite API client.

Author: xunfeng
Date:   2022/04/28
Email:  xunfeng@test.com
"""
import sys
import json
import time
import sqlite3

from quant.utils import tools
#from quant.config import config



if len(sys.argv) > 1:
    config_file = sys.argv[1]
else:
    config_file = None
SERVER_ID = "0"


def loads(config_file=None,configsrc="file"):
    global SERVER_ID
    configures = {}
    if configsrc == "file" : 
        if config_file:
            try:
                with open(config_file,encoding='utf-8') as f:
                    data = f.read()
                    configures = json.loads(data)
            except Exception as e:
                print(e)
                exit(0)
            if not configures:
                print("config json file error!")
                exit(0)
    else:
        if config_file:
            configures = config_file
            if not configures:
                print("config json file error!")
                exit(0)
    SERVER_ID=configures.get("SERVER_ID", "0")

loads(config_file,configsrc="file")      


class Sqlitedb:
    """ Create a MongoDB connection cursor.

    Args:
        db: DB name.
        collection: Collection name.
    """
    global SERVER_ID

    def __init__(self,robotId=SERVER_ID,Logtocmd = False):
        """ Initialize. """
        self.robotid = robotId
        self.Logtocmd = Logtocmd
        self._conn = sqlite3.connect(self.robotid+".db3",check_same_thread=False)
        self._cursor = self._conn.cursor()
        try:
            #创建表
            self._cursor.execute('CREATE TABLE `cfg` (`k` TEXT NOT NULL PRIMARY KEY UNIQUE, `v` TEXT NOT NULL, `date` INTEGER NOT NULL)')
            self._cursor.execute('CREATE TABLE `chart` ( `id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, `seriesId` INTEGER, `data` TEXT, `date` INTEGER NOT NULL)')
            self._cursor.execute('CREATE TABLE `kvdb` ( `k` TEXT NOT NULL UNIQUE, `v`TEXT NOT NULL, PRIMARY KEY(k))')
            self._cursor.execute('CREATE TABLE `log` ( `id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, `platformId` INTEGER, `orderId` TEXT, `logType` INTEGER NOT NULL, `price` REAL, `amount` REAL, `extra` TEXT, `instrument` TEXT, `direction` TEXT, `date` INTEGER NOT NULL)')
            self._cursor.execute('CREATE TABLE `profit` ( `id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, `profit` REAL, `extra` TEXT, `date` INTEGER NOT NULL)')
            #创建索引
            self._cursor.execute('CREATE INDEX chart_date on chart(date)')
            self._cursor.execute('CREATE INDEX log_date on log(date)')
            self._cursor.execute('CREATE INDEX profit_date on profit(date)')
            self._conn.commit()
            print(self.robotid,"机器人信息表创建成功")
        except BaseException as e:
            print("创建机器人信息表失败")
            print("错误信息：", e)

    def new_cursor(self):
        """ Generate a new cursor.

        Args:
            db: New db name.
            collection: New collection name.

        Return:
            cursor: New cursor.
        """
        cursor = self._conn.cursor()
        return cursor
    def profit(self,data,params=""):
        """储存利润数据"""
        try:
            if not data:
                return
            data = float(data)
            if not params=="&":
                print("收益:",data)
            entities = (data,params,int(time.time()*1000))
            self._cursor.execute("insert into profit(profit,extra,date) values (?, ?,?)", entities)
            self._conn.commit()
        except BaseException as e:
            print("插入利润数据失败")
            print("错误信息：", e)

    def LogProfit(self,data,params=""):
        """储存利润数据"""
        try:
            if not data:
                return
            data = float(data)
            if not params=="&":
                print("收益:",data)
            entities = (data,params,int(time.time()*1000))
            self._cursor.execute("insert into profit(profit,extra,date) values (?, ?,?)", entities)
            self._conn.commit()
        except BaseException as e:
            print("插入利润数据失败")
            print("错误信息：", e)
    
    def profitreset(self,datanumber):
        """设置利润数据条数"""
        try:
            if not datanumber and (not datanumber==0):
                return
            datanumber = int(datanumber)
            sql = "delete from profit where id not in (select id from profit order by id desc limit 0,"+str(datanumber)+")"
            self._cursor.execute(sql)
            self._conn.commit()
        except BaseException as e:
            print("设置利润数据条数失败")
            print("错误信息：", e)
    
    def LogProfitReset(self,datanumber):
        """设置利润数据条数"""
        try:
            if not datanumber and (not datanumber==0):
                return
            datanumber = int(datanumber)
            sql = "delete from profit where id not in (select id from profit order by id desc limit 0,"+str(datanumber)+")"
            self._cursor.execute(sql)
            self._conn.commit()
        except BaseException as e:
            print("设置利润数据条数失败")
            print("错误信息：", e)

            

    def updatestatus(self,data):
        """储存状态栏信息,有则更新，无则插入"""
        try:
            if not data:
                return
            data = str(data)
            entities = ("status",data,int(time.time()*1000))
            self._cursor.execute("replace into cfg(k,v,date) values (?, ?,?)", entities)
            self._conn.commit()
        except BaseException as e:
            print("插入状态信息失败")
            print("错误信息：", e)
    
    def LogStatus(self,data):
        """储存状态栏信息,有则更新，无则插入"""
        try:
            if not data:
                return
            data = str(data)
            entities = ("status",data,int(time.time()*1000))
            self._cursor.execute("replace into cfg(k,v,date) values (?, ?,?)", entities)
            self._conn.commit()
        except BaseException as e:
            print("插入状态信息失败")
            print("错误信息：", e)

    def _log(self,*args, **kwargs):
        _log_msg = ""
        for l in args:
            if type(l) == tuple:
                ps = str(l)
            else:
                try:
                    ps = "%r" % l
                except:
                    ps = str(l)
            if type(l) == str:
                _log_msg += ps[1:-1] + " "
            else:
                _log_msg += ps + " "
        if len(kwargs) > 0:
            _log_msg += str(kwargs)
        return _log_msg

    def Log(self,*args, **kwargs):
        """储存日志信息"""
        try:
            data = self._log(*args, **kwargs)
            if self.Logtocmd:
                print(data)
            entities = (5,data[:10000],int(time.time()*1000))
            self._cursor.execute("insert into log(logType,extra,date) values (?,?,?)", entities)
            self._conn.commit()                       
        except BaseException as e:
            print("插入日志数据失败")
            print("错误信息：", e)
    
    def Logreset(self,datanumber):
        """设置利润数据条数"""
        try:
            if not datanumber and (not datanumber==0):
                return
            datanumber = int(datanumber)
            sql = "delete from log where id not in (select id from log order by id desc limit 0,"+str(datanumber)+")"
            self._cursor.execute(sql)
            self._conn.commit()
        except BaseException as e:
            print("设置日志数据条数失败")
            print("错误信息：", e)
    
    def LogReset(self,datanumber):
        """设置利润数据条数"""
        try:
            if not datanumber and (not datanumber==0):
                return
            datanumber = int(datanumber)
            sql = "delete from log where id not in (select id from log order by id desc limit 0,"+str(datanumber)+")"
            self._cursor.execute(sql)
            self._conn.commit()
        except BaseException as e:
            print("设置日志数据条数失败")
            print("错误信息：", e)
    
    def _G(self,k, v="None"):
        """储存KV表"""
        #tyshow_to_front为保留字段,显示信息会显示到前端
        try:
            if k is None:#删除所有全局变量
                self._cursor.execute("DELETE FROM kvdb")
                self._conn.commit()
            else:
                if v is None:#删除值
                    self._cursor.execute("DELETE FROM kvdb WHERE k = ?",(k,))
                    self._conn.commit()                      
                else:
                    if v=="None":#读取
                        sql = "SELECT v from kvdb WHERE k = ?"
                        self._cursor.execute(sql,(k,))
                        rows = self._cursor.fetchall() 
                        for row in rows:  
                            return json.loads(row[0])
                    else:
                        if k and v:#插入或更新   
                            entities=(k,json.dumps(v))
                            self._cursor.execute("replace into kvdb(k,v) values (?, ?)", entities)                         
                            self._conn.commit()                         
        except BaseException as e:
            print("操作KV数据失败")
            print("错误信息：", e)

    def DBExec(self,sql):
        """SQLite数据库自定义命令"""
        try:
            self._cursor.execute(sql)
            self._conn.commit()
            res = self._cursor.fetchall()
        except BaseException as e:
            print("SQLite数据库自定义命令执行失败:",e)
            res = None
        return res
    
    def updatechart(self,chartcfg):
        """储存图表配置,有则更新，无则插入"""
        try:
            if not chartcfg:
                return
            chartcfg = str(chartcfg)
            entities = ("chart",chartcfg,int(time.time()*1000))
            self._cursor.execute("replace into cfg(k,v,date) values (?, ?,?)", entities)
            self._conn.commit()
        except BaseException as e:
            print("插入图表配置失败")
            print("错误信息：", e)

    def chartadd(self,datas):
        """ 储存图表数据 """
        """data:[[series.length, [time, index]],[series.length, [time, index]]]"""
        try:
            if not datas:
                return
            timestamp = int(time.time()*1000)
            for seriesdata in datas:
                seriesId = seriesdata[0]
                data = json.dumps(seriesdata[1])
                entities = (int(seriesId),data,timestamp)
                self._cursor.execute("insert into chart(seriesId,data,date) values (?, ?,?)", entities)
            self._conn.commit()
        except BaseException as e:
            print("插入图表数据失败")
            print("错误信息：", e)

    def chartreset(self,datanumber):
        """设置图表数据条数"""
        try:
            if not datanumber and (not datanumber==0):
                return
            datanumber = int(datanumber)
            sql = "delete from chart where id not in (select id from chart order by id desc limit 0,"+str(datanumber)+")"
            self._cursor.execute(sql)
            self._conn.commit()
        except BaseException as e:
            print("设置图表数据条数失败")
            print("错误信息：", e)

    def getcommand(self):
        """获取交互命令"""
        try:
            sql = 'select v from cfg WHERE k = ?'
            self._cursor.execute(sql,("cmd",))
            rows = self._cursor.fetchall() 
            self._cursor.execute("DELETE FROM cfg WHERE k = ?",("cmd",))
            self._conn.commit()
            for row in rows:  
                return row[0]            
        except BaseException as e:
            print("获取交互命令失败")
            print("错误信息：", e)
        return None
    def Getcommand(self):
        """获取交互命令"""
        try:
            sql = 'select v from cfg WHERE k = ?'
            self._cursor.execute(sql,("cmd",))
            rows = self._cursor.fetchall() 
            self._cursor.execute("DELETE FROM cfg WHERE k = ?",("cmd",))
            self._conn.commit()
            for row in rows:  
                return row[0]            
        except BaseException as e:
            print("获取交互命令失败")
            print("错误信息：", e)
        return None
    def Getrobotstatus(self):
        """
        获取机器人循环状态码
        机器人状态:
        运行中:1
        停止中:2
        已停止:3
        有错误:4
        """
        try:
            sql = 'select v from cfg WHERE k = ?'
            self._cursor.execute(sql,("robotloopcmd",))
            rows = self._cursor.fetchall() 
            self._cursor.execute("DELETE FROM cfg WHERE k = ?",("robotloopcmd",))
            self._conn.commit()
            for row in rows:  
                return int(row[0])            
        except BaseException as e:
            print("获取机器人循环状态码失败")
            print("错误信息：", e)
        return None


    