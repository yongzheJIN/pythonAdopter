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
            return True
        else:
            return False


    @classmethod
    def concat_sql_from_binlog_event(cls, cursor, binlog_event, row=None, e_start_pos=None, flashback=False,
                                     no_pk=False):
        if flashback and no_pk:
            raise ValueError('only one of flashback or no_pk can be True')
        if not (isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent)
                or isinstance(binlog_event, DeleteRowsEvent) or isinstance(binlog_event, QueryEvent)):
            raise ValueError('binlog_event must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent')

        if isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent) \
                or isinstance(binlog_event, DeleteRowsEvent):
            cls.generate_sql_pattern(binlog_event)

    @classmethod
    def compare_items(cls, items):
        # caution: if v is NULL, may need to process
        (k, v) = items
        if v is None:
            return '`%s` IS %%s' % k
        else:
            return '`%s`=%%s' % k

    @classmethod
    def fix_object(cls, value):
        """Fixes python objects so that they can be properly inserted into SQL queries"""
        if isinstance(value, set):
            value = ','.join(value)
        else:
            return value

    @classmethod
    def confirmSchem(cls, database,tableName, values):
        if os.path.exists(f"./schemaGroup/{database}/{tableName}.json"):
            with open(f"./schemaGroup/{database}/{tableName}.json", encoding="utf-8") as fp:
                tableSchema = json.load(fp)
                if len(values) == len(tableSchema[tableName]["columns"]):
                    # 主键和所有键值
                    priColumns = {}
                    corColumns = {tableSchema[tableName]['columns'][i]: values[i] for i in range(len(values))}
                    for i in tableSchema[tableName]["primaryKey"]:
                        priColumns[i] = corColumns[i]
                    return corColumns, priColumns
                else:
                    raise TypeError(f"{tableName}的表结构没有还原成原始的样子")

    @classmethod
    def generate_sql_pattern(cls, binlog_event, replace=False):
        if isinstance(binlog_event, DeleteRowsEvent):
            columns, priColumns = cls.confirmSchem(binlog_event.schema, binlog_event.table,
                                                   list(binlog_event.rows[0]['values'].values()))
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ' AND '.join(map(cls.compare_items, list(priColumns.items())))
            )
            values = priColumns.values()
            print(111, template, values)
        elif isinstance(binlog_event, WriteRowsEvent):
            # 获取字段对应
            columns, priColumns = cls.confirmSchem(binlog_event.schema, binlog_event.table,list(binlog_event.rows[0]['values'].values()))
            if not replace:
                values = columns.values()
                template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                    binlog_event.schema, binlog_event.table,
                    ', '.join(map(lambda i: f"`{i}`", columns.keys())),
                    ', '.join(['%s'] * len(values))
                )
            else:
                values = columns.values()
                template = 'REPLACE INTO `{0}`.`{1}`({2}) values({3})'.format(
                    binlog_event.schema, binlog_event.table,
                    ', '.join(map(lambda i: f"`{i}`", columns.keys())),
                    ', '.join(['%s'] * len(values))
                )
            print(111, template, values)

        elif isinstance(binlog_event, UpdateRowsEvent):
            columns, priColumns = cls.confirmSchem(binlog_event.schema,binlog_event.table,
                                                   list(binlog_event.rows[0]['after_values'].values()))
            if not replace:
                template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                    binlog_event.schema, binlog_event.table,
                    ', '.join(['`%s`=%%s' % x for x in columns.keys()]),
                    ' AND '.join(map(cls.compare_items, list(priColumns.items()))))
                values = list(columns.values())
                values.extend(priColumns.values())
            else:
                values = columns.values()
                template = 'REPLACE INTO `{0}`.`{1}`({2}) values({3})'.format(
                    binlog_event.schema, binlog_event.table,
                    ', '.join(map(lambda i: f"`{i}`", columns.keys())),
                    ', '.join(['%s'] * len(values))
                )

            print(111,template, values)
