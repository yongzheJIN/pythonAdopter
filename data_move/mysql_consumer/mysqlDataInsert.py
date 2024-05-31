import threading
from concurrent.futures import ThreadPoolExecutor

import tqdm

from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector

INSERT_SQLS_LOCK = threading.Lock()

class MysqlTransactionInsert:
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

    def mutlti_line_insert_thread(self,INSERT_SQLS,work_num=10):
        """
        多线程插入数据
        """
        # 多线程插入
        with ThreadPoolExecutor(max_workers=work_num) as executor:
            futures = []
            while INSERT_SQLS:
                sql = INSERT_SQLS[0]
                with INSERT_SQLS_LOCK:
                    INSERT_SQLS.pop(0)
            futures.append(executor.submit(self.insert, sql))
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Error occurred while inserting data: {e}")

    def insert(self, sql):
        # 插入数据
        print(1)
        # with mysqlConnector(ip=self.ip, port=self.port, user=self.user,
        #                     password=self.password, database=None) as connector:
        #     cursor = connector.cursor()
        #     cursor.execute(sql[0], sql[1])
        #     connector.commit()
        # with mysqlConnector(ip=self.ip, port=self.port, user=self.user,
        #                     password=self.password,database=None) as connector:
        #     cursor = connector.cursor()
        #     cursor.execute(sql[0], sql[1])
        #     connector.commit()
