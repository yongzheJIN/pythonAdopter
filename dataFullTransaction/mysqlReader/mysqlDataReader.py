import threading
from concurrent.futures import ThreadPoolExecutor
import tqdm

from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector

# 因为sql是会在两个类之间传递，所以需要一个全局变量
INSERT_SQLS = []


class MysqlTransactionQuery:
    # 这是一个mysql读取类，他会一个多线程的方式读取mysql的数据
    def __init__(self, ip: str, port: int, user: str, password: str, databaseList: list):
        """
        :param ip: mysql的ip
        :param port: mysql的端口
        :param user: mysql的用户名
        :param password: mysql的密码
        :param databaseList: 需要迁移的数据库列表
        """
        self.ip = ip
        self.port = port
        self.user = user
        self.password = password
        self.conn = None
        # 所有表格的名字例如xex_plus.person
        self.tableList = []
        self.databaseList = databaseList
        # 多线程的锁
        self.table_list_lock = threading.Lock()
        self.falseTable = []

    def get_table_list(self):
        # 获取所有的表格名字
        for database in self.databaseList:
            with mysqlConnector(ip=self.ip, port=self.port, user=self.user,
                                password=self.password,
                                database=database, fixDatabase=True) as connector:
                cursors = connector.cursor()
                cursors.execute("show tables")
                tables = cursors.fetchall()
                for table in tables:
                    self.tableList.append(f"{database}.{table[0]}")

    def get_table_construction(self):
        """
        获取所有表的建表语句
        """
        sql = []
        for table in self.tableList:
            database, table = table.split(".")[0], table.split(".")[1]
            with mysqlConnector(ip=self.ip, port=self.port, user=self.user,
                                password=self.password,
                                database=database, fixDatabase=True) as connector:
                cursor = connector.cursor()
                cursor.execute(f"SHOW CREATE TABLE {table}")
                create_table = cursor.fetchall()
                sql.append(create_table[0][1].replace(f"`{create_table[0][0]}`", f"`{database}`.`{table}`"))
        return sql

    def get_table_data(self, table: str):
        # 获取表格的数据
        database, table = table.split(".")[0], table.split(".")[1]
        with mysqlConnector(ip=self.ip, port=self.port, user=self.user,
                            password=self.password,
                            database=database, fixDatabase=True) as connector:
            cursor = connector.cursor()
            # 获取列名
            cursor.execute(f"SHOW COLUMNS FROM {table}")
            columns = [f"`{column[0]}`" for column in cursor.fetchall()]

            cursor.execute(f"select * from {table}")
            data = cursor.fetchall()
            for i in range(0, len(data), 1000):  # 每1000条数据为一个子列表
                sub_data = data[i:i + 1000]
                placeholders = ', '.join(['(' + ', '.join(['%s'] * len(row)) + ')' for row in sub_data])  # 创建占位符字符串
                insert_sql = f"INSERT INTO `{database}`.`{table}` ({', '.join(columns)}) VALUES {placeholders};"
                INSERT_SQLS.append(
                    [insert_sql, [item for sublist in sub_data for item in sublist]])  # 将SQL语句和数据作为一个元组存储

    def read_data(self, work_number):
        with ThreadPoolExecutor(max_workers=work_number) as executor:  # 创建一个最大线程数为10的线程池
            futures = []
            # 控制线程任务的对象
            while self.tableList:
                table = self.tableList[0]
                futures.append((table, executor.submit(self.get_table_data_and_print, table)))  # 提交任务到线程池
                with self.table_list_lock:
                    self.tableList.remove(self.tableList[0])

            for table, future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"{table}读取失败{e}")
                    self.falseTable.append(table)
        return INSERT_SQLS

    def get_table_data_and_print(self, table):
        print(f"{threading.current_thread().name}正在读取{table}的数据")
        self.get_table_data(table)
        print(f"{threading.current_thread().name}{table}的数据读取完")


