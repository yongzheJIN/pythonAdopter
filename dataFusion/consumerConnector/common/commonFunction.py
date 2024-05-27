# 组装Insert、Update和Delete三种functions
import json
import os
import re


def organizedFunction(event_type, funcInsert, funcUpdate, funcDelete, table, InsertTableList, DeleteTableList,
                      data, useReplace, res, mapAll):
    """
    把res传入进来时为了实现原有语句的rewrite
    """
    # 根据是INSERT、DELETE和UPDATE情况分开处理，每个都有自己不同的处理逻辑
    # insert的逻辑
    if event_type == 1:
        # 如果他不存在table_list里面说明他是第一次插入
        if mapAll:
            current = __defaultInsertFunction(data, useReplace) if not funcInsert else funcInsert(data,
                                                                                                  useReplace)
        else:
            current = __indicationInsert(data, useReplace) if not funcInsert else funcInsert(data,
                                                                                             useReplace)
        if not useReplace:
            if table not in InsertTableList['table']:
                InsertTableList['table'].append(table)
                InsertTableList['index'].append(len(res))
                res.append(current)
            else:
                # 获取表在 InsertTableList 中的索引
                index = InsertTableList["index"][InsertTableList['table'].index(table)]

                # 合并两个 INSERT 语句的值部分
                combined_values = ', '.join(['%s'] * len(current[1]))
                # 将当前行的值添加到原始值中
                res[index][0] = res[index][0][:-1]
                res[index][0] = f"{res[index][0]},({combined_values});"
                res[index][1].extend(current[1])
                # 打印合并后的结果
        else:
            if current:
                res.append(current)
    # 更新逻辑
    elif event_type == 2:
        ## 如果是更新的话都是一个单独的语句直接塞入到res中即可
        if mapAll == True:
            current = __defaultUpdateFunction(data, useReplace=useReplace) if not funcUpdate else funcUpdate(data,
                                                                                                             useReplace)
        else:
            current = __indicationUpdateFunction(data, useReplace=useReplace) if not funcUpdate else funcUpdate(data,
                                                                                                                useReplace)
        if current:
            res.append(current)
    # 删除逻辑
    elif event_type == 3:
        # 如果他不存在table_list里面说明他是第一次删除不能做拼接
        if mapAll:
            current = __defaultDeleteFunction(data) if not funcDelete else funcDelete(data)
        else:
            current = __indicationDeleteFunction(data) if not funcDelete else funcDelete(data)
        if current:
            res.append(current)
    else:
        print('--------------------')
    return res


# 生成Insert语句
def __defaultInsertFunction(data, useReplace):
    """
    data:{
            table:表名,
            primary_list:[{"name":"","value":""}]
            database:库名
            before:{"name":"1"},
            after:{"name":"2"}
        }
    """
    # insert 逻辑
    table = data['table']
    database = data['database']

    res_columns = ""
    res_values = ""
    query_values = []
    for key, value in data['data']['after'].items():
        res_columns = res_columns + "`" + key + "`" + ","
        res_values = res_values + "%s,"

        # 如果值是 'NULL'，则将 None 添加到 query_values，否则添加原始值
        query_values.append(None if value == 'NULL' else value)

    # 去除结尾的","
    res_columns = res_columns[:-1]
    res_values = res_values[:-1]
    if useReplace:
        insert_sql = f"REPLACE INTO {database}.{table} ({res_columns}) VALUES({res_values});"
    else:
        insert_sql = f"INSERT INTO {database}.{table} ({res_columns}) VALUES({res_values});"

    # 返回包含 SQL 语句和参数值的列表
    return [insert_sql, query_values]


def __indicationInsert(data, useReplace):
    # 判断对象是不是单引号包裹
    pattern = re.compile(r"^'.*'$")
    # 找有没有配置文件
    if os.path.exists(f"config/consumerConfig/tableGroup/{data['table']}.json"):
        with open(f"config/consumerConfig/tableGroup/{data['table']}.json") as fp:
            mapConfig = json.load(fp)
        table = list(mapConfig.keys())[0]
        database = data['database'] if bool(mapConfig[table].get("targetDatabase", -1) == -1) else mapConfig[table].get(
            "targetDatabase")
        res_columns = ""
        res_values = ""
        query_values = []
        # 从配置表里面读取数据
        for key, value in mapConfig[table]['data'].items():
            # res_columns = res_columns + key + "," if bool(pattern.match(value)) else res_columns + value + ","
            # 获取目标字段的columns名称组
            res_columns = res_columns + key + ","
            #  获取占位符组
            res_values = res_values + "%s,"
            # 如果单引号包裹说明你设置的默认值，如果没有就去源数据里面取
            if bool(pattern.match(value)):
                query_values.append(value[1:-1])
            else:
                query_values.append(None if data['data']['after'][value] == "NULL" else data['data']['after'][value])
        # 去除结尾的","
        res_columns = res_columns[:-1]
        res_values = res_values[:-1]
        # 根据一开始是否采取了replace组成sql,你不需要单条执行每条语句的效率,因为代码中默认就做了rewriteCompress,所有可以组合在一起的sql都会放在一起，且在一个transaction里面提交
        if useReplace:
            insert_sql = f"REPLACE INTO {database}.{table} ({res_columns}) VALUES({res_values});"
        else:
            insert_sql = f"INSERT INTO {database}.{table} ({res_columns}) VALUES({res_values});"
        # 返回包含 SQL 语句和参数值的列表
        return [insert_sql, query_values]
    else:
        return []


# 生成update语句
def __defaultUpdateFunction(data, useReplace):
    # update逻辑
    database = data['database']
    table = data['table']
    if useReplace:
        # 获取执行的对象表名
        table = data['table']
        res_columns = ""
        res_values = ""
        query_values = []
        # 遍历形成columns的列表和values的%s
        for key, value in data['data']['after'].items():
            res_columns = res_columns + "`" + key + "`" + ","
            res_values = res_values + "%s,"

            # 如果值是 'NULL'，则将 None 添加到 query_values，否则添加原始值
            query_values.append(None if value == 'NULL' else value)
        # 去除结尾的","
        res_columns = res_columns[:-1]
        res_values = res_values[:-1]

        update_sql = f"REPLACE INTO {database}.{table} ({res_columns}) VALUES({res_values});"
        # 返回包含 SQL 语句和参数值的列表
        return [update_sql, query_values]
    else:
        set_clause = ""
        query_values = []

        # 遍历形成 SET 子句
        for key, value in data['data']['after'].items():
            set_clause += f"{key} = %s, "

            # 如果值是 'NULL'，则将 None 添加到 query_values，否则添加原始值
            query_values.append(None if value == 'NULL' else value)

        # 去除结尾的", "
        set_clause = set_clause[:-2]
        # 准备 WHERE 子句
        where_clause = ' AND '.join([f"{item['name']} = %s" for item in data['data']['primary_List']])
        where_values = [item['value'] for item in data['data']['primary_List']]

        # 合并 SET 子句和 WHERE 子句的参数值
        query_values += where_values
        # 准备完整的 SQL 语句
        update_sql = f"UPDATE {database}.{table} SET {set_clause} WHERE {where_clause};"

        # 返回包含 SQL 语句和参数值的列表
        return [update_sql, tuple(query_values)]


def __indicationUpdateFunction(data, useReplace):
    # 判断对象是不是单引号包裹
    pattern = re.compile(r"^'.*'$")
    res_columns = ""
    res_values = ""
    query_values = []
    if os.path.exists(f"config/consumerConfig/tableGroup/{data['table']}.json"):
        with open(f"config/consumerConfig/tableGroup/{data['table']}.json") as fp:
            mapConfig = json.load(fp)
        table = list(mapConfig.keys())[0]
        database = data['database'] if bool(mapConfig[table].get("targetDatabase", -1) == -1) else mapConfig[table].get(
            "targetDatabase")
        # 采取了replace模式
        if useReplace == True:
            for key, value in mapConfig[table]['data'].items():
                res_columns = res_columns + key + ","
                res_values = res_values + "%s,"
                pattern = re.compile(r"^'.*'$")

                # 如果单引号包裹说明你设置的默认值，如果没有就去源数据里面取
                if bool(pattern.match(value)):
                    query_values.append(value[1:-1])
                else:
                    query_values.append(
                        None if data['data']['after'][value] == "NULL" else data['data']['after'][value])
                # 返回包含 SQL 语句和参数值的列表
            res_columns = res_columns[:-1]
            res_values = res_values[:-1]
            update_sql = f"REPLACE INTO {database}.{table} ({res_columns}) VALUES({res_values});"
            return [update_sql, query_values]
        # 没有采取replace模式
        else:
            database = data['database'] if bool(mapConfig[table].get("targetDatabase", -1) == -1) else \
                mapConfig[data['table']].get("targetDatabase")
            set_clause = ""
            query_values = []
            # 遍历形成 SET 子句
            for key, value in mapConfig.get(table)['data'].items():
                set_clause += f"{key} = %s, "
                # 如果有默认值就走默认值
                if bool(pattern.match(value)):
                    query_values.append(value[1:-1])
                else:
                    # 如果值是 'NULL'，则将 None 添加到 query_values，否则添加原始值
                    query_values.append(
                        None if data['data']['after'][value] == "NULL" else data['data']['after'][value])
            set_clause = set_clause[:-2]
            # 准备 WHERE 子句
            where_clause = ' AND '.join([f"{key} = %s " for key, value in mapConfig.get(table)['primaryKey'].items()])
            where_values = [data['data']['after'][value] if not bool(pattern.match(value)) else value[1:-1] for
                            key, value
                            in mapConfig[table]['primaryKey'].items()]
            # 合并 SET 子句和 WHERE 子句的参数值
            query_values += where_values
            # 准备完整的 SQL 语句
            update_sql = f"UPDATE {database}.{table} SET {set_clause} WHERE {where_clause};"
            return [update_sql, query_values]
    # 判断目标是不是再mapConfig里面
    else:
        return []


# 生成DELETE语句不需要对照mapconfig完完全全的影子库
def __defaultDeleteFunction(data):
    database = data['database']
    primary_list = data['data']['primary_List']
    table = data['table']
    where_clauses = ' AND '.join([f"`{item['name']}` = %s" for item in primary_list])
    delete_sql = f"DELETE from {database}.{table} where {where_clauses};"
    return [delete_sql, [i['value'] for i in primary_list]]


# 生成DELETE需要对照mapConfig的部分
def __indicationDeleteFunction(data):
    # 判断对象是不是单引号包裹
    pattern = re.compile(r"^'.*'$")
    table = data['table']
    if os.path.exists(f"config/consumerConfig/tableGroup/{table}.json"):
        with open(f"config/consumerConfig/tableGroup/{table}.json") as fp:
            mapConfig = json.load(fp)
        table = list(mapConfig.keys())[0]
        database = data['database'] if bool(mapConfig[table].get("targetDatabase", -1) == -1) else mapConfig[table].get(
            "targetDatabase")
        where_clauses = ' AND '.join(
            [f"{key} = %s" if not bool(pattern.match(value)) else f"{key} = %s" for key, value in
             mapConfig[table]['primaryKey'].items()])
        where_values = [data['data']['after'][value] if not bool(pattern.match(value)) else value[1:-1] for key, value
                        in mapConfig[table]['primaryKey'].items()]
        delete_sql = f"DELETE from {database}.{table} where {where_clauses};"
        return [delete_sql, where_values]
    else:
        return []
