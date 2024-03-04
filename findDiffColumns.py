from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector

# 需要找到不同的表
tableGroup = ['']

### 找到源库和目标库的字段差异 ###
def findDiffernt(originHost, originPort, originUser, originPassword, originDatabase, tableName, targetHost, targetPort,
                 targetUser, targetPassword, targetDatabase):
    originColumns = []
    targetColumns = []
    with mysqlConnector(ip=originHost, port=originPort, user=originUser,
                        password=originPassword,
                        database=originDatabase) as connector:
        cursors = connector.cursor()
        sql = f"""DESCRIBE {originDatabase}.{tableName}"""
        cursors.execute(sql)
        for i in cursors.fetchall():
            originColumns.append(str(i[0]))

    with mysqlConnector(ip=targetHost, port=targetPort, user=targetUser,
                        password=targetPassword,
                        database=targetDatabase) as connector:
        cursors = connector.cursor()
        sql = f"""DESCRIBE {targetDatabase}.{tableName}"""
        cursors.execute(sql)
        for i in cursors.fetchall():
            targetColumns.append(str(i[0]))
    result = set(targetColumns) - set(originColumns)
    if result != {"client_id"} and result != {"clientId"} and result != {"ClientId"}:
        print(tableName, result)


# 目标数据库连接方式，和源库连接方式
for i in tableGroup:
    findDiffernt(originHost="120.71.147.86", originPort=3306, originUser="root", originPassword="zkxbx@2011",
                 originDatabase="xex_plus",
                 targetHost="120.71.147.86", targetPort=3306, targetUser="root",
                 targetPassword="zkxbx@2011", targetDatabase="civil_admin", tableName=i)
