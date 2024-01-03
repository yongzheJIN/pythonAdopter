import json
import time
# kafka连接模块
from confluent_kafka import Consumer, KafkaError
# 导入查询语句生成模块
from dataFusion.consumerConnector.common.commonFunction import organizedFunction
# 数据库连接模块
from dataFusion.mysqlConnector.mysqlConnector import mysqlConnector


class kafkaConnector:
    def __init__(self, kakfkaHost, kafkaPort, kafkaTopic, kafkaGroup, mysqlip, mysqlport, mysqluser, mysqlpassword,
                 mysqlpdatabase, useReplace, maxKafkaPollingSize=100, kafkaConsumerModel=1, mysqlread_timeout=20,
                 kafkaCosumerModel="earliest"):
        ### kafka的设置
        self.kakfkaHost = kakfkaHost
        self.kafkaPort = kafkaPort
        self.kafkaTopic = kafkaTopic
        self.kafkaGroup = kafkaGroup
        # 1代表如果不存在任何旗帜，就从最开始消费，如果存在就从消费点开始，其他的任何值都取latest只从最末尾去值
        self.kafkaConsumerModel = kafkaCosumerModel
        # 是否开始拉取数据
        self.pollingStatus = False
        # 每次拉取的最大数量
        self.maxKafkaPollingSize = maxKafkaPollingSize
        # 持久化kafka连接
        self.kafkaConsumer: Consumer
        ### mysql的设置
        self.mysqlip = mysqlip
        self.mysqlport = mysqlport
        self.mysqluser = mysqluser
        self.mysqlpassword = mysqlpassword
        self.mysqldatabase = mysqlpdatabase
        self.useReplace = useReplace

    def __enter__(self):
        try:
            # 测试mysql的连接
            with mysqlConnector(ip=self.mysqlip, port=self.mysqlport, user=self.mysqluser, password=self.mysqlpassword,
                                database=self.mysqldatabase):
                print("连接mysql数据库成功")
            # 创建 Kafka 消费者配置
            consumer_conf = {
                'bootstrap.servers': f"{self.kakfkaHost}:{self.kafkaPort}",
                'group.id': self.kafkaGroup,
                # 每次都从offset的位置开始消费，而不是忘记消费失败数据
                'auto.offset.reset': 'earliest',
                # 不要自动提交事件
                'enable.auto.commit': False,
            }
            # 创建 Kafka 消费者
            self.kafkaConsumer = Consumer(consumer_conf)
            # 订阅主题
            self.kafkaConsumer.subscribe([self.kafkaTopic])
            print("连接kafka成功")
            return self
        except Exception as e:
            raise ConnectionError(f"连接kafka失败{e}")

    # 装饰器函数,让你可以自己写自己内部的插入和更新操作
    def listenToPort(self, funcInsert, funcUpdate, funcDelete, mapAll: False):
        """
        参数说明
        funcInsert自己的Insert逻辑(会接受到event_type,header,database,table)
        funcUpdate自己的Update逻辑(会接受到event_type,header,database,table)
        内部变量说明
        event_type: 事件名称
        header:
        database: 修改的数据库

        """
        while True:
            # 控制每次拉取的数据size
            msgs = []
            res = []
            # 持续从 Kafka 主题中拉取消息
            continue_polling = True
            while continue_polling:
                msg = self.kafkaConsumer.poll(timeout=2)
                if msg is None:
                    # 如果数据伪空直接跳过结束次轮循环
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # 没有更多的消息可消费，停止继续拉取
                        continue_polling = False
                    else:
                        print(f"Error: {msg.error()}")
                        break
                else:
                    # ... 处理消息的逻辑
                    msgs.append(msg)
                    # 如果达到一定的处理条数，也停止继续拉取
                    if len(msgs) > self.maxKafkaPollingSize:
                        continue_polling = False
            # sql统一存储的数组
            # 所有insert执行过的table 这里主要用于insert，把多个insert拼成一个{table:['xex_home_order'],index:['1']}
            InsertTableList = {"table": [], "index": []}
            DeleteTableList = {"table": [], "index": []}
            # 获取原始数据集
            for msg in msgs:
                originalDatas = json.loads(msg.value().decode("utf-8"))
                # 存储整个数据
                formdata = {}
                # 获取事件类型,数据库名,数据表
                # 于canalTcp适配 1-update, 2-insert,3-delete
                if originalDatas['type'] == 'INSERT':
                    entryType = 1
                elif originalDatas['type'] == "UPDATE":
                    entryType = 2
                elif originalDatas['type'] == "DELETE":
                    entryType = 3
                else:
                    break
                database = originalDatas['database']
                table = originalDatas['table']
                # 获取主键Dict
                formdata["primary_List"] = [{"name": i, "value": originalDatas['data'][0][i]} for i in
                                            originalDatas["pkNames"]]
                # 组成before数据和after数据
                formdata['after'] = {key: values if values != None else "NULL" for key, values in
                                     originalDatas['data'][0].items()}
                # 如果是更新,就要记录他之前的数据,如果不是直接设置为NONE,保持数据格式统一
                if entryType == 2:
                    formdata['before'] = {key: values if values != None else "NULL" for key, values in
                                          originalDatas['data'][0].items()}
                else:
                    formdata['before'] = None
                data = dict(
                    database=database,
                    table=table,
                    event_type=entryType,
                    data=formdata,
                )
                # 把insert,update和delete的逻辑都放在一起
                res = organizedFunction(event_type=entryType, funcInsert=funcInsert, funcUpdate=funcUpdate,
                                        funcDelete=funcDelete, table=table, InsertTableList=InsertTableList,
                                        DeleteTableList=DeleteTableList, res=res,
                                        data=data, useReplace=self.useReplace, mapAll=mapAll)
            if res:
                try:
                    # 传入数据，如果数据消费成功递交ack位置如果失败把mysqlConnector 和canalClient rollback
                    with mysqlConnector(ip=self.mysqlip, port=self.mysqlport, user=self.mysqluser,
                                        password=self.mysqlpassword,
                                        database=self.mysqldatabase) as connector:
                        # 制作cursor操作对象
                        cursor = connector.cursor()
                        for i in res:
                            cursor.execute(i[0], i[1])
                        connector.commit()
                    # 成功之后递交我的ACK位置
                    self.kafkaConsumer.commit()
                    print("事件提交成功", res)
                    continue
                except Exception as e:
                    print("----------------")
                    print({f"失败：回滚时间轴。"
                           f"失败原因:{e}"
                           f"失败sql组:{res}"
                           f"失败sql:{i}"})
                    # 如果失败就停止程序
                    break
            time.sleep(1)


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.kafkaConsumer.close()
