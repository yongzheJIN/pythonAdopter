from pymysqlreplication import BinLogStreamReader

mysql_settings = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': '123456'}

stream = BinLogStreamReader(connection_settings = mysql_settings, server_id=100)

for binlogevent in stream:
    binlogevent.dump()

stream.close()