import json

from dataRepair.bin2sql.binlog2sql import binlogSQL




if __name__ == "__main__":
    with open("./dataRepair/bin2sql/setting.json") as fp:
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
    sqlParser.getDCL()
    sqlParser.process_binlog(database, table)