import time

import pymysql
import subprocess


def generateData(table):
    result = ""
    columns, key_columns = findColumnsOfsql(table)
    print("获取表结构")
    # 读取原结构
    if len(key_columns) == 0:
        print(table, "没有找到任何主键")
    else:
        with open("./test.ktr", encoding="utf-8") as f:
            print("开始仿制文件")
            data = f.readline()
            while data:
                # 替换33查询sql
                if "{33sql}" in data:
                    data = data.replace("{33sql}",
                                        f'select * from {table} where update_time>"2023-12-12 00:00:00" and update_time&lt;"2023-12-15 6:00:00"')
                if "{76sql}" in data:
                    data = data.replace("{76sql}",
                                        f'select * from {table} where update_time>"2023-12-12 00:00:00" ')
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
            # time.sleep(1)
            #
            # # 执行命令
            # output = subprocess.check_output("E:\\Kettle\\data-integration\\pan.bat /file:G:\\pythonAdopter\\new.ktr /level:Basic")
            # print('output',output)


def findColumnsOfsql(targetTable):
    original_cursor = pymysql.connect(
        host="192.168.1.152",
        port=30633,
        user="root",
        password="Zkxbx@2011",
        database="xex_plus"
    )
    cursors = original_cursor.cursor()
    sql = f"""DESCRIBE xex_plus.{targetTable}"""
    cursors.execute(sql)
    original_columns = []
    key_columns = []
    for i in cursors.fetchall():
        original_columns.append(str(i[0]))
        if str(i[3]) == "PRI":
            key_columns.append(i[0])
    original_cursor.close()
    return original_columns, key_columns


# 查询所有的table
def getALLtables():
    original_cursor = pymysql.connect(
        host="192.168.1.152",
        port=30633,
        user="root",
        password="Zkxbx@2011",
        database="xex_plus"
    )

    cursors = original_cursor.cursor()
    sql = f"""SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_schema = "xex_plus";"""
    cursors.execute(sql)
    original_columns = []
    for i in cursors.fetchall():
        if str(i).startswith("QRTZ") == False:
            original_columns.append(str(i[0]))
    original_cursor.close()
    return original_columns


def mainFunction():
    tables = getALLtables()
    for index, table in enumerate(tables):
        generateData(table)
        print(index + 1, "/", len(tables) + 1)


if __name__ == "__main__":

    generateData("sys_store_institution")
    # findColumnsOfsql("xex_home_wallet_detail")
