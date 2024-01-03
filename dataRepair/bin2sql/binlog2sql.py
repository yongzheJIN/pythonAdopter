import re
import subprocess

from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector

# 解析binlog的语句
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent




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
            'C:\ProgramData\MySQL\MySQL Server 8.0\Data\MS-LGYDPYKSLKYB-bin.000058'
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
        insert_pattern = re.compile(r'###\s+INSERT\s+INTO\s+.*?\s+SET\s+(.*?)\s+###', re.DOTALL)
        insert_matches = insert_pattern.findall(str(binlog_content))

        for match in insert_matches:
            # 输出匹配到的 INSERT 语句
            print(f"INSERT INTO ... SET {match}")






if __name__ == "__main__":
    sqlParser = binlogSQL(ipAddress="127.0.0.1", port=3306, userName="root", password="123456", database="xex_plus_qd")
    sqlParser.read_binlog(r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqlbinlog.exe",)
