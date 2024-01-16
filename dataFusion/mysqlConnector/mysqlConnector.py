import pymysql


class mysqlConnector:
    def __init__(self, ip, port, user, password, database, fixDatabase=False):
        ## 类型判断
        if not isinstance(port, int) and not ip:
            raise TypeError("port必须是数字")
        # 数据库连接方式
        self.ip = ip
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        # 数据库持久连接
        self.connector: pymysql.connect
        # 锁定数据库
        self.fixDatabase = fixDatabase

    # 确保连接的关闭
    def __enter__(self):
        try:
            if self.fixDatabase:
                self.connector = pymysql.connect(port=self.port, host=self.ip, user=self.user, password=self.password,
                                                 database=self.database)
            else:
                self.connector = pymysql.connect(port=self.port, host=self.ip, user=self.user, password=self.password)
            return self.connector
        except Exception as e:
            raise ConnectionError(f"数据库连接失败,{e}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 如果部分失败就把所有的都rollback
        if exc_type is not None:
            self.connector.rollback()
        self.connector.close()
