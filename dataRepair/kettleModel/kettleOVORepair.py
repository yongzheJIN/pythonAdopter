import subprocess
import time

import pymysql
from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector


class kettleRepairGenerate:
    def __init__(self, originalServer, originalPort, originalUser, originalPassword, originalDatabase, targetServer,
                 targetPort, targetUser, targetPassword, targetDatabase, repairImmidiate: False):
        # 源数据库的的连接数据
        self.originalSever = originalServer
        self.originalPort = originalPort
        self.originalUser = originalUser
        self.originalPassword = originalPassword
        self.originalDatabase = originalDatabase
        # 目标数据库的连接方式
        self.targetSever = targetServer
        self.targetPort = targetPort
        self.targetUser = targetUser
        self.targetPassword = targetPassword
        # 是否立即修复
        self.repairImmidiate = repairImmidiate

    def shadowDatabaseRepair(self, repairStartTime, repairEndTime):
        """
        原始数据有，目标数据库没有就往里面插入
        repairStartTime: 修复数据的开始update_time
        repairEndTime: 修复数据的的结束update_time
        """
        tables = self.getALLtables()

        def generateData(table):
            result = ""
            columns, key_columns = self.findColumnsOfsql(table)
            print("获取表结构")
            # 读取原结构
            if len(key_columns) == 0:
                print(table, "没有找到任何主键")
            else:
                with open(f"./{table}.ktr", encoding="utf-8") as f:
                    print("开始仿制文件")
                    data = f.readline()
                    while data:
                        # 替换33查询sql
                        if "{33sql}" in data:
                            data = data.replace("{33sql}",
                                                f'select * from {table} where update_time>{repairStartTime} and update_time&lt;{repairEndTime}')
                        if "{76sql}" in data:
                            data = data.replace("{76sql}",
                                                f'select * from {table} where update_time>{repairStartTime} ')
                        if "{comprise_value}" in data:
                            res = ""
                            for i in columns:
                                res += f"      <value>{i}</value>\n"
                            columns = columns[:-1]
                            data = data.replace("{comprise_value}", res)
                        if "{targetTable}" in data:
                            data = data.replace("{targetTable}", table)
                        if "{update_columns}" in data:
                            res = ""
                            for i in columns:
                                simgleTemplate = f"""      <value>\n        <name>{i}</name>\n        <rename>{i}</rename>\n        <update>Y</update>\n      </value>\n"""
                                res = res + simgleTemplate
                            data = data.replace("{update_columns}", res)
                        if "{update_key}" in data:
                            res = ""
                            for i in key_columns:
                                simgleTemplate = f"""      <key>\n        <name>{i}</name>\n        <field>{i}</field>\n        <condition>=</condition>\n        <name2/>\n      </key>\n"""
                                res = res + simgleTemplate
                            res = res[:-1]
                            data = data.replace("{update_key}", res)
                        if "{comprise_key}" in data:
                            res = ""
                            for i in key_columns:
                                simgleTemplate = f"      <key>{i}</key>\n"
                                res = res + simgleTemplate
                            res = res[:-1]
                            data = data.replace("{comprise_key}", res)
                        result = result + data
                        data = f.readline()
                        # 输出目标结构
                    with open("./new.ktr", "w", encoding="utf-8") as f:
                        f.write(result)
                    print("仿制结束")

        for index, table in enumerate(tables):
            generateData(table)
            print(f"完成度：{index + 1}, '/', {len(tables) + 1}"
                  f"正在处理表:{table}")
            #
            # # 执行命令
            if self.repairImmidiate:
                output = subprocess.check_output(
                    "E:\\Kettle\\data-integration\\pan.bat /file:G:\\pythonAdopter\\new.ktr /level:Basic")
                print('output', output)
                time.sleep(1)

    def findColumnsOfsql(self, targetTable):
        """
        找到某一个表格的所哟字段，包括哪些是primary key
        """
        with mysqlConnector(ip=self.originalSever, port=self.originalPort, user=self.originalUser,
                            password=self.originalPassword, database=self.originalDatabase) as connector:
            cursors = connector.cursor()
            sql = f"""DESCRIBE xex_plus.{targetTable}"""
            cursors.execute(sql)
            original_columns = []
            key_columns = []
            for i in cursors.fetchall():
                original_columns.append(str(i[0]))
                if str(i[3]) == "PRI":
                    key_columns.append(i[0])
            return original_columns, key_columns

    # 查询所有的table
    def getALLtables(self):
        """
        获取所有的表格
        """
        with mysqlConnector(ip=self.originalSever, port=self.originalPort, user=self.originalUser,
                            password=self.originalPassword, database=self.originalDatabase) as connector:
            cursors = connector.cursor()
            sql = f"""SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_schema = "xex_plus" AND table_type = 'BASE TABLE';"""
            cursors.execute(sql)
            original_columns = []
            for i in cursors.fetchall():
                original_columns.append(str(i[0]))
            return original_columns
