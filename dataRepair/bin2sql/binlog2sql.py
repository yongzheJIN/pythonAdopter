import os
import re
import subprocess
import datetime

from pymysqlreplication.event import QueryEvent

from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector

# 解析binlog的语句
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent


# 利用mysqlbinlog工具读取binlog信息转换成row模式的数据,并进行数据修复。
class binlogSQL:
    def __init__(self, port, ipAddress, userName, password, database):
        """
        数据库连接参数
        """
        self.port = port
        self.ipAddress = ipAddress
        self.user = userName
        self.password = password
        self.database = database
        ## 验证数据库连接情况
        with mysqlConnector(ip=self.ipAddress, port=self.port, user=self.user, password=self.password,
                            database=self.database) as connector:
            print("数据库连接方式验证成功")

    def read_binlog(self,mysql_binlog_path):
        command = [
            mysql_binlog_path,
            '--base64-output=DECODE-ROWS',
            '--verbose',
            # '--start-datetime=' + self.start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            # '--stop-datetime=' + self.end_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            # Add your other MySQL connection parameters here, e.g., '-h', 'localhost', '-u', 'username', '-pPassword'
            'C:\ProgramData\MySQL\MySQL Server 8.0\Data\MS-LGYDPYKSLKYB-bin.000001'
        ]

        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            print(f"Error reading binlog: {result.stderr.decode('utf-8')}")
        else:
            # 使用 'utf-8' 编码解码输出
            binlog_content = result.stdout
            # 处理 binlog 内容，提取 INSERT 和 UPDATE 语句
            self.extract_insert_update_statements(binlog_content)


    def extract_insert_update_statements(self,binlog_content):
        for i in str(binlog_content).split("\\n"):
            print(i)
        insert_pattern = re.compile(r'###\s+INSERT\s+INTO\s+.*?\s+SET\s+(.*?)\s+###', re.DOTALL)
        insert_matches = insert_pattern.findall(str(binlog_content))

        for match in insert_matches:
            # 输出匹配到的 INSERT 语句
            print(f"INSERT INTO ... SET {match}")

    def process_binlog(self):
        stream = BinLogStreamReader(connection_settings={'host': "127.0.0.1", 'port': 3306, 'user': "root", 'passwd': "123456", 'charset': 'utf8'},server_id=1,
                                    log_file="MS-LGYDPYKSLKYB-bin.000001",resume_stream=True, blocking=True)
        flag_last_event = False
        e_start_pos, last_pos = stream.log_pos, stream.log_pos
        # to simplify code, we do not use flock for tmp_file.
        tmp_file = self.create_unique_file('%s.%s' % ("127.0.0.1", 3306))
        for binlog_event in stream:
            print(binlog_event)


        return True

    def create_unique_file(self,filename):
        version = 0
        result_file = filename
        # if we have to try more than 1000 times, something is seriously wrong
        while os.path.exists(result_file) and version < 1000:
            result_file = filename + '.' + str(version)
            version += 1
        if version >= 1000:
            raise OSError('cannot create unique file %s.[0-1000]' % filename)
        return result_file

    def concat_sql_from_binlog_event(self,cursor, binlog_event, row=None, e_start_pos=None, flashback=False, no_pk=False):
        if flashback and no_pk:
            raise ValueError('only one of flashback or no_pk can be True')
        if not (isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent)
                or isinstance(binlog_event, DeleteRowsEvent) or isinstance(binlog_event, QueryEvent)):
            raise ValueError('binlog_event must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent')
        sql = ''
        if isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent) \
                or isinstance(binlog_event, DeleteRowsEvent):
            pattern = self.generate_sql_pattern(binlog_event, row=row, flashback=flashback, no_pk=no_pk)
            print(111)
        #     sql = cursor.mogrify(pattern['template'], pattern['values'])
        #     time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
        #     sql += ' #start %s end %s time %s' % (e_start_pos, binlog_event.packet.log_pos, time)
        # elif flashback is False and isinstance(binlog_event, QueryEvent) and binlog_event.query != 'BEGIN' \
        #         and binlog_event.query != 'COMMIT':
        #     if binlog_event.schema:
        #         sql = 'USE {0};\n'.format(binlog_event.schema)
        #     sql += '{0};'.format(fix_object(binlog_event.query))

    def generate_sql_pattern(self,binlog_event, row=None, flashback=False, no_pk=False):
        template = ''
        values = []
        if flashback is True:
            if isinstance(binlog_event, WriteRowsEvent):
                print("1",binlog_event,binlog_event.schema,binlog_event.table)
                # template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                #     binlog_event.schema, binlog_event.table,
                #     ' AND '.join(map(compare_items, row['values'].items()))
                # )
                # values = map(fix_object, row['values'].values())
            elif isinstance(binlog_event, DeleteRowsEvent):
                print("2", binlog_event, binlog_event.schema, binlog_event.table)
            elif isinstance(binlog_event, UpdateRowsEvent):
                print("3", binlog_event, binlog_event.schema, binlog_event.table)
                # template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                #     binlog_event.schema, binlog_event.table,
                #     ', '.join(['`%s`=%%s' % x for x in row['before_values'].keys()]),
                #     ' AND '.join(map(compare_items, row['after_values'].items())))
                # values = map(fix_object, list(row['before_values'].values()) + list(row['after_values'].values()))
        else:
            None
            # if isinstance(binlog_event, WriteRowsEvent):
            #     if no_pk:
            #         # print binlog_event.__dict__
            #         # tableInfo = (binlog_event.table_map)[binlog_event.table_id]
            #         # if tableInfo.primary_key:
            #         #     row['values'].pop(tableInfo.primary_key)
            #         if binlog_event.primary_key:
            #             row['values'].pop(binlog_event.primary_key)
            #
            #     template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
            #         binlog_event.schema, binlog_event.table,
            #         ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
            #         ', '.join(['%s'] * len(row['values']))
            #     )
            #     values = map(fix_object, row['values'].values())
            # elif isinstance(binlog_event, DeleteRowsEvent):
            #     template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
            #         binlog_event.schema, binlog_event.table, ' AND '.join(map(compare_items, row['values'].items())))
            #     values = map(fix_object, row['values'].values())
            # elif isinstance(binlog_event, UpdateRowsEvent):
            #     template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
            #         binlog_event.schema, binlog_event.table,
            #         ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]),
            #         ' AND '.join(map(compare_items, row['before_values'].items()))
            #     )
            #     values = map(fix_object, list(row['after_values'].values()) + list(row['before_values'].values()))

        # return {'template': template, 'values': list(values)}


if __name__ == "__main__":
    sqlParser = binlogSQL(ipAddress="127.0.0.1", port=3306, userName="root", password="123456", database="xex_plus_qd")
    # sqlParser.read_binlog(r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqlbinlog.exe",)
    sqlParser.process_binlog()
