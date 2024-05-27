import json
import os

from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent


class binlogUtilis:
    @classmethod
    def is_dml_event(cls, event):
        # 是不是DML事件
        if isinstance(event, WriteRowsEvent) or isinstance(event, UpdateRowsEvent) or isinstance(event,
                                                                                                 DeleteRowsEvent):
            return 1
        elif (isinstance(event, QueryEvent) and event.query.lower().startswith("alter table")) \
                or (isinstance(event, QueryEvent) and event.query.lower().startswith("drop table")) \
                or (isinstance(event, QueryEvent) and event.query.lower().startswith("create table")):
            return 2
        else:
            return -1

    @classmethod
    def compare_items(cls, items):
        # caution: if v is NULL, may need to process
        (k, v) = items
        if v is None:
            return '`%s` IS %%s' % k
        else:
            return '`%s`=%%s' % k

    @classmethod
    def confirmSchem(cls, database, tableName, values):
        if os.path.exists(f"./schemaGroup/{database}/{tableName}.json"):
            with open(f"./schemaGroup/{database}/{tableName}.json", encoding="utf-8") as fp:
                tableSchema = json.load(fp)
                if len(values) == len(tableSchema[tableName]["columns"]):
                    # 主键和所有键值
                    primary_key_columns = {}
                    other_columns = {tableSchema[tableName]['columns'][i]: values[i] for i in range(len(values))}
                    for i in tableSchema[tableName]["primaryKey"]:
                        primary_key_columns[i] = other_columns[i]
                    return other_columns, primary_key_columns
                else:
                    raise TypeError(f"{tableName}的表结构没有还原成原始的样子")

    @classmethod
    def generate_sql_pattern(cls, binlog_event, database, table, replace=False):
        # 判断这个是不是目前监听的表
        if database and binlog_event.schema not in database:
            return
        if table and f"{binlog_event.schema}.{binlog_event.table}" not in table:
            return
        # 生成sql语句
        if isinstance(binlog_event, DeleteRowsEvent):
            columns, priColumns = cls.confirmSchem(binlog_event.schema, binlog_event.table,
                                                   list(binlog_event.rows[0]['values'].values()))
            values = priColumns.values()
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ' AND '.join(map(cls.compare_items, list(priColumns.items())))
            )
            return [template, list(values)]
        elif isinstance(binlog_event, WriteRowsEvent):
            # 获取字段对应
            columns, priColumns = cls.confirmSchem(binlog_event.schema, binlog_event.table,
                                                   list(binlog_event.rows[0]['values'].values()))
            if not replace:
                values = columns.values()
                template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                    binlog_event.schema, binlog_event.table,
                    ', '.join(map(lambda i: f"`{i}`", columns.keys())),
                    ', '.join(['%s'] * len(values))
                )
                return [template, list(values)]
            else:
                values = columns.values()
                template = 'REPLACE INTO `{0}`.`{1}`({2}) values({3})'.format(
                    binlog_event.schema, binlog_event.table,
                    ', '.join(map(lambda i: f"`{i}`", columns.keys())),
                    ', '.join(['%s'] * len(values))
                )
                return [template, list(values)]

        elif isinstance(binlog_event, UpdateRowsEvent):
            columns, priColumns = cls.confirmSchem(binlog_event.schema, binlog_event.table,
                                                   list(binlog_event.rows[0]['after_values'].values()))
            if not replace:
                template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                    binlog_event.schema, binlog_event.table,
                    ', '.join(['`%s`=%%s' % x for x in columns.keys()]),
                    ' AND '.join(map(cls.compare_items, list(priColumns.items()))))
                values = list(columns.values())
                values.extend(priColumns.values())
                return [template, values]
            else:
                values = columns.values()
                template = 'REPLACE INTO `{0}`.`{1}`({2}) values({3})'.format(
                    binlog_event.schema, binlog_event.table,
                    ', '.join(map(lambda i: f"`{i}`", columns.keys())),
                    ', '.join(['%s'] * len(values))
                )
                return template, values
