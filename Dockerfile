FROM python:3.10.14-bullseye
WORKDIR /app
COPY . /app
RUN pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple/ flask canal-python kafka-python requests pyMySQL==1.1.0 confluent_kafka==2.3.0 protobuf==3.20.1
CMD ["python3", "mainDataFusion.py"]
