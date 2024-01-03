import json

from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector


class mapConfigGeneration:
    def __init__(self, targetHost: str, targetPort: int, targetUser: str, targetPassword: str, targetDatabase: str,
                 originHost: str, originPort: int, originUser: str, originPassword: str, originDatabase: str,
                 tables: list[str]):
        # 目标库数据库连接方式
        self.targetHost = targetHost
        self.targetPort = targetPort
        self.targetUser = targetUser
        self.targetPassword = targetPassword
        self.targetDatabase = targetDatabase
        # 源库数据库连接方式
        self.originHost = originHost
        self.originPort = originPort
        self.originUser = originUser
        self.originPassword = originPassword
        self.originDatabase = originDatabase
        # 需要转出哪些表的
        if not isinstance(tables, list):
            raise TypeError("tables必须是list")
        self.tables = tables

    def __enter__(self):
        # 验证服务器连接可否
        with mysqlConnector(ip=self.targetHost, port=self.targetPort, user=self.targetUser, password=self.targetPassword,
                            database=self.targetDatabase):
            print("目标库测试连接成功")
        with mysqlConnector(ip=self.originHost, port=self.originPort, user=self.originUser, password=self.originPassword,
                            database=self.originDatabase):
            print("源库测试连接成功")

    # target:1源库,2:目标库
    def getTableStructure(self, target, targetName) -> tuple[list[str], list[str]]:
        """
        方法:获取表的结构
        target: enum 1->源数据库 2->目标数据库
        targetName:str 表名字
        return :List[str]->字段列表
        """
        if target == 1:
            with mysqlConnector(ip=self.targetHost, port=self.targetPort, user=self.targetUser,
                                password=self.targetPassword,
                                database=self.targetDatabase) as connector:
                cursors = connector.cursor()
                sql = f"""DESCRIBE {self.targetDatabase}.{targetName}"""
                cursors.execute(sql)
                columns = []
                primarkey = []
                for i in cursors.fetchall():
                    if str(i[3]) == "PRI":
                        primarkey.append(str(i[0]))
                    columns.append(str(i[0]))
                return columns, primarkey
        elif target == 2:
            with mysqlConnector(ip=self.originHost, port=self.originPort, user=self.originUser,
                                password=self.originPassword,
                                database=self.originDatabase) as connector:
                cursors = connector.cursor()
                sql = f"""DESCRIBE {self.originDatabase}.{targetName}"""
                cursors.execute(sql)
                columns = []
                primarkey = []
                for i in cursors.fetchall():
                    if str(i[3]) == "PRI":
                        primarkey.append(str(i[0]))
                    columns.append(str(i[0]))
                return columns, primarkey
        else:
            raise TypeError("target只能为1和2 1:->源表,2->目标表")

    def generateFile(self, type: int) -> None:
        """
        type: 1->只要目标表有就当做需要从源表同步
        type: 2->只要源表有就当做需要

        return: None 直接创建文件不要任何返回
        """
        if type == 1:
            for table in self.tables:
                res = {}
                res[table] = {
                    'primaryKey': {

                    },
                    'data': {

                    },
                    'targetDatabase': self.targetDatabase
                }
                columns,primaryKey = self.getTableStructure(1, table)
                for i in columns:
                    if i=="client_id" or i=="clientId" or i=="ClientId" or i=="Clientid":
                        res[table]["data"][i] = "'634f21fdsa234dsf'"
                    else:
                        res[table]["data"][i] = i
                for i in primaryKey:
                    if i=="client_id" or i=="clientId" or i=="ClientId" or i=="Clientid":
                        res[table]["primaryKey"][i] = "'634f21fdsa234dsf'"
                    else:
                        res[table]["primaryKey"][i] = i
                with open(f'..\\..\\config\\consumerConfig\\tableGroup\\{table}.json','w') as fp:
                    json.dump(res, fp,indent=2)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            return f"意外退出mapGeneration:原因是{exc_val}"


if __name__ == "__main__":
    mapGeneratation = mapConfigGeneration(
        targetHost="192.168.205.250",
        targetPort=3306,
        targetUser="root",
        targetPassword="zkxbx@2011",
        targetDatabase="civil_admin",

        originHost="192.168.205.250",
        originPort=3306,
        originUser="root",
        originPassword="zkxbx@2011",
        originDatabase="xex_plus",

        tables=["employee","institution_plus_st","institution_plus","institution","person_base","person"]
    )
    mapGeneratation.generateFile(1)
