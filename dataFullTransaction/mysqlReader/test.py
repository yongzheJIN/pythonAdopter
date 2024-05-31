from dataFullTransaction.mysqlReader.mysqlDataReader import MysqlTransactionQuery
from dataFullTransaction.mysqlConsumer.mysqlDataInsert import MysqlTransactionInsert
if __name__ == "__main__":
    mysqlReader = MysqlTransactionQuery(ip="127.0.0.1", port=3306, user="root", password="Zkxbx@2011",
                                        databaseList=["jrc_mongo_data"])
    mysqlReader.get_table_list()
    sqls = mysqlReader.get_table_construction()
    INSERT_SQL = mysqlReader.read_data(10)
    consumer = MysqlTransactionInsert(ip="192.168.1.222", port=3306, user="root", password="Zkxbx@2011")
    consumer.build(sqls)
    consumer.mutlti_line_insert_thread(INSERT_SQL, 20)
