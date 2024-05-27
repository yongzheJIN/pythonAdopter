import json
import os

from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector
from dataRepair.bin2sql.binlog2sqlUtilils import binlogUtilis

# 解析binlog的语句
from pymysqlreplication import BinLogStreamReader


# 利用mysql binlog工具读取binlog信息转换成row模式的数据,并进行数据修复。
class binlogSQL:
    def __init__(self, port, ipAddress, userName, password, database, startTimeStamp, endTimeStamp, logFile):
        """
        数据库连接参数
        """
        # 数据库连接方式
        self.port = port
        self.ipAddress = ipAddress
        self.user = userName
        self.password = password
        self.database = database
        # 读取的时间轴范围
        self.startTimeStamp = startTimeStamp
        self.endTimeStamp = endTimeStamp
        # 设置的开始阅读的binlog文件
        self.logFile = logFile
        # 用于控制binlog是否开始解析
        self.startFlag = False
        ## 验证数据库连接情况
        with mysqlConnector(ip=self.ipAddress, port=self.port, user=self.user, password=self.password,
                            database=self.database):
            print("数据库连接方式验证成功")

    def getDCL(self):
        # 因为binlog文件里面没有columns的信息所以需要提前获取
        with open("./setting.json") as fp:
            res = json.load(fp)
            if res['mapAll'] == 0:
                with mysqlConnector(ip=self.ipAddress, port=self.port, user=self.user, password=self.password,
                                    database=self.database) as connector:
                    cursor = connector.cursor()
                    # 获取所有的库
                    cursor.execute("show databases")
                    database_list = cursor.fetchall()
                    for j in database_list:
                        # 获取所有的表
                        cursor.execute(f"use {j[0]}")
                        cursor.execute(f"show tables;")
                        table_list = cursor.fetchall()
                        for i in table_list:
                            self.writeTable(cursor, j[0], i[0])
            elif res['mapAll'] == 1:
                with mysqlConnector(ip=self.ipAddress, port=self.port, user=self.user, password=self.password,
                                    database=self.database, fixDatabase=True) as connector:
                    cursor = connector.cursor()
                    try:
                        database_list = res["database"]
                    except:
                        raise ResourceWarning("mapAll代表按照数据库过滤,那必须要需要配置database属性")
                    for j in database_list:
                        # 获取所有的表
                        cursor.execute(f"use {j};show tables;")
                        table_list = cursor.fetchall()
                        for i in table_list:
                            self.writeTable(cursor, j, i[0])
            elif res['mapAll'] == 2:
                table_list = res['tables']
                with mysqlConnector(ip=self.ipAddress, port=self.port, user=self.user, password=self.password,
                                    database=self.database, fixDatabase=True) as connector:
                    cursor = connector.cursor()
                    for i in table_list:
                        i[0], i[1] = i.split(",")
                        self.writeTable(cursor=cursor, database=i[0], table=i[1])

    def writeTable(self, cursor, database, table):
        columns = []
        primaryKey = []
        cursor.execute(f"DESCRIBE `{table}`")
        res = cursor.fetchall()
        for column in res:
            # ('id', 'bigint', 'NO', 'PRI', None, 'auto_increment')
            columns.append(column[0])
            if column[3] == "PRI":
                primaryKey.append(column[0])
        output = {}
        output[table] = {}
        output[table]["primaryKey"] = primaryKey
        output[table]['columns'] = columns
        if not os.path.exists(f".\\schemaGroup\\{database}"):
            os.makedirs(f".\\schemaGroup\\{database}")
        with open(f".\\schemaGroup\\{database}\\{table}.json", 'w') as fp:
            json.dump(output, fp, indent=2)

    def process_binlog(self, database, table):
        stream = BinLogStreamReader(
            connection_settings={'host': self.ipAddress, 'port': self.port, 'user': self.user, 'passwd': self.password,
                                 'charset': 'utf8'}, server_id=99,
            log_file=self.logFile, log_pos=4, resume_stream=True, blocking=True)
        for binlog_event in stream:
            # 判断是不是dml事件
            if not self.startFlag:
                if binlog_event.timestamp > self.startTimeStamp:
                    self.startFlag = True
                    print("-------------开始")
            if binlog_event.timestamp > self.endTimeStamp:
                break
            types = binlogUtilis.is_dml_event(binlog_event)
            # 1 代表他是DML事件，2 代表他是DDL事件
            if types == 1 and self.startFlag:
                result = binlogUtilis.generate_sql_pattern(binlog_event, database, table)
                if result:
                    try:
                        with mysqlConnector(ip=self.ipAddress, port=self.port, user=self.user, password=self.password,
                                            database=self.database) as connector:
                            cursor = connector.cursor()
                            cursor.execute(result)
                    except Exception as e:
                        raise TypeError(f"失败的timestamp:{binlog_event.timestamp},执行失败:{result}")
            elif types == 2 and self.startFlag and binlog_event.schema.decode('utf-8') in database and not table:
                # 如果是数据库DDL的话
                with mysqlConnector(ip=self.ipAddress, port=self.port, user=self.user, password=self.password,
                                    database=self.database) as connector:
                    cursor = connector.cursor()
                    cursor.execute([binlog_event.query])

        return True

    def create_unique_file(self, filename):
        version = 0
        result_file = filename
        # if we have to try more than 1000 times, something is seriously wrong
        while os.path.exists(result_file) and version < 1000:
            result_file = filename + '.' + str(version)
            version += 1
        if version >= 1000:
            raise OSError('cannot create unique file %s.[0-1000]' % filename)
        return result_file


if __name__ == "__main__":
    # 如果不配table就是全部
    ## 根据配置模式生成配置文件
    with open("setting.json") as fp:
        res = json.load(fp)
        startTimestamp = res['startTimeStamp']
        endTimeStamp = res['endTimestamp']
        logFile = res['binLogFileName']
        database = res['database'] if 'database' in res else None
        table = res['table'] if 'table' in res else None
        if not database and not table:
            raise ResourceWarning("database和table必须要有一个不是空值的，如果都不为空则采取table优先模式")

    if table and database:
        check_flag = input("因为database和table都不为空，所以采取table优先模式，是否继续(y/n):")
        if check_flag == "n":
            raise ResourceWarning("用户取消操作")
    else:
        check_flag = input("采取了table模式，是否继续(y/n):") if table else input("采取了database模式，是否继续(y/n):")
        if check_flag == "n":
            raise ResourceWarning("用户取消操作")

    sqlParser = binlogSQL(ipAddress="127.0.0.1", port=3306, userName="root", password="Zkxbx@2011", database=database,
                          startTimeStamp=startTimestamp, endTimeStamp=endTimeStamp, logFile=logFile)
    # 先检查能不能成功创建Debzium一样的字段对应
    # sqlParser.getDCL()
    # sqlParser.read_binlog(r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqlbinlog.exe",)
    sqlParser.process_binlog(database, table)

    # sqlParser.getDCL()
