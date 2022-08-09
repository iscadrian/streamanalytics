from kafka import KafkaProducer, KafkaConsumer
import time
import random
import json
import threading
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
import mariadb
import sys


class SalesOrder:
    def __init__(self):
        self.__orderId = 1000

    def setOrderId(self, orderId):
        self.__orderId = orderId

    def getOrderId(self):
        return self.__orderId

    def setProduct(self, product):
        self.__product = product

    def getProduct(self):
        return self.__product

    def setQuantity(self, quantity):
        self.__quantity = quantity

    def getQuantity(self):
        return self.__quantity

    def setPrice(self, price):
        self.__price = price

    def getPrice(self):
        return self.__price


class RowPrinter:
    def open(self, partition_id, epoch_id):
        print("Opened %d, %d" % (partition_id, epoch_id))
        return True

    def process(self, row):
        print(row)

    def close(self, error):
        print("Closed with error: %s" % str(error))


class MariaDBWriter:
    def open(self, partition_id, epoch_id):
        print("Connecting to MariaDB...")
        self.updateMgr = MariaDBManager()
        self.updateMgr.setup()
        return True

    def process(self, row):
        print("inserting records in order_summary")
        self.updateMgr.insertSummary(str(row[0][0]), row[1], str(row[2]))

    def close(self, error):
        self.updateMgr.teardown()
        print("Closed with error: %s" % str(error))


class MariaDBManager:
    def setup(self):
        try:
            self.conn = mariadb.connect(
                host="127.0.0.1", port=3306, user="streaming", password="streaming"
            )
            self.cur = self.conn.cursor()
        except mariadb.Error as e:
            print(f"Error connecting to the databse: {e}")
            sys.exit(1)

    def teardown(self):
        try:
            self.conn.close()
        except mariadb.Error as e:
            print(e)

    def insertSummary(self, timestamp, product, value):
        try:
            sql = (
                "INSERT INTO streaming.order_summary "
                + "(INTERVAL_TIMESTAMP, PRODUCT, TOTAL_VALUE) VALUES "
                + "( '"
                + timestamp
                + "',"
                + " '"
                + product
                + "',"
                + value
                + ")"
            )
            self.cur.execute(sql)
            self.conn.commit()
        except mariadb.Error as e:
            print(f"An error has ocurred {e}")


def thread_genorder():
    topic = "streaming.orders.input"

    ANSI_RESET = "\u001B[0m"
    ANSI_GREEN = "\u001B[32m"
    ANSI_PURPLE = "\u001B[35m"
    ANSI_BLUE = "\u001B[34m"

    products = ["keyboard", "mouse", "monitor"]
    prices = [25.00, 10.5, 140.00]

    orderId = round(time.time())

    for i in range(1, 21):
        print(f"Record:{i}")
        so = SalesOrder()
        so.setOrderId(orderId)

        orderId = orderId + 1

        randval = random.randint(0, len(products) - 1)
        so.setProduct(products[randval])
        so.setPrice(prices[randval])
        so.setQuantity(random.randint(1, 10))

        reckey = str(so.getOrderId())
        value = json.dumps(so.__dict__)
        byte_value = value.encode("utf-8")

        producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
        producer.send(topic, byte_value)

        print(
            ANSI_PURPLE
            + "Kafka Orders Stream Generator : Sending Event : "
            + ","
            + value
            + ANSI_RESET
        )

        time.sleep(2)


def main():

    x = threading.Thread(target=thread_genorder)
    x.start()

    spark = SparkSession.builder.appName("StreamAnalytics").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "1")

    schema = T.StructType(
        [
            T.StructField("_SalesOrder__orderId", T.IntegerType(), True),
            T.StructField("_SalesOrder__product", T.StringType(), True),
            T.StructField("_SalesOrder__price", T.DoubleType(), True),
            T.StructField("_SalesOrder__quantity", T.IntegerType(), True),
        ]
    )

    rawOrdersDf = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("startingOffsets", "earliest")
        .option("subscribe", "streaming.orders.input")
        .load()
    )

    ordersDf = (
        rawOrdersDf.selectExpr("CAST(value AS STRING)")
        .select(F.from_json(F.col("value"), schema).alias("order"))
        .select(
            [
                "order._SalesOrder__orderId",
                "order._SalesOrder__product",
                "order._SalesOrder__price",
                "order._SalesOrder__quantity",
            ]
        )
    )

    orderTest = (
        ordersDf.withColumn("timestamp", F.current_timestamp())
        .select(
            F.col("timestamp"),
            F.col("_SalesOrder__product").alias("product"),
            F.col("_SalesOrder__price").alias("value"),
        )
    )

    # ordersDf.writeStream.foreach(RowPrinter()).start()
    # orderTest.writeStream.foreach(MariaDBWriter()).start()

    windowedSummary = (
        ordersDf.selectExpr(
            "_SalesOrder__product as product",
            "_SalesOrder__quantity * _SalesOrder__price as value",
        )
        .withColumn("timestamp", F.current_timestamp())
        .withWatermark("timestamp", "2 seconds")
        .groupBy(F.window("timestamp", "2 seconds"), "product")
        .agg(F.sum("value"))
    )

    windowedSummary.writeStream.foreach(MariaDBWriter()).start()


if __name__ == "__main__":
    main()
