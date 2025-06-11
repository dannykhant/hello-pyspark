import uuid

from pyspark import SparkConf, SparkContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, mean


def count_words():
    """Basic Example"""

    x = "data/input.txt"
    conf = SparkConf().setAppName("App1").setMaster("local[*]")
    sc = SparkContext.getOrCreate(conf)
    y = sc.textFile(x).cache()
    cnt = y.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    cnt.saveAsTextFile("data/output-" + str(uuid.uuid4()))


def count_stream():
    """Streaming Example"""

    sp = SparkSession.builder.appName("App2").master("local[2]").getOrCreate()
    lines = sp.readStream.format("socket").option("host", "localhost").option("port", "9999").load()
    words = lines.select(explode(split(lines.value, " "))).alias("word")
    wc = words.groupby("col").count()
    query = wc.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()


def find_spent():
    """
    ML Example
    Problem: To find Yearly Amount spent (dependent variable)
    based on info such as "Avg Session Length","Time on App","Time on Website",
    "Length of Membership" [independent variables]
    To check schema: data.printSchema()
    To see data: df2.show()
    """

    sp = SparkSession.builder.appName("App3").master("local").getOrCreate()
    data = (sp.read.option("header", "true").option("inferSchema", "true")
            .format("csv").load("data/EcommerceCustomers1.csv"))
    df = data.select(data["Yearly Amount Spent"].alias("label"), data["Avg Session Length"], data["Time on App"],
                     data["Time on Website"], data["Length of Membership"])
    # For matrix format
    vs = VectorAssembler().setInputCols(["Avg Session Length", "Time on App", "Time on Website",
                                         "Length of Membership"]).setOutputCol("features")
    # Transform features
    df2 = vs.transform(df).select("label", "features")
    # LR model
    lr = LinearRegression()
    lr_model = lr.fit(df2)
    training_summary = lr_model.summary
    training_summary.residuals.show()


def query_data():
    """SQL Example"""

    sp = SparkSession.builder.appName("App4").master("local").getOrCreate()
    # json
    json_data = sp.read.json("data/employees.json")
    json_data.select("name", json_data["salary"] + 5).filter(json_data["salary"] > 4000).show()
    # csv
    csv_data = sp.read.format("csv").option("inferSchema", "true").option("header", "true").load("data/BankData.csv")
    over60 = csv_data.filter(csv_data["age"] > 60).count()
    total = float(csv_data.count())
    over60_rate = (over60 / total) * 100.0
    print("Over60 Rate :", over60_rate)
    csv_data.select(mean(csv_data["balance"])).show()
    # Temp View
    csv_data.createOrReplaceTempView("bank_data")
    under20 = sp.sql("select * from bank_data where age < 20 limit 10")
    under20.write.save("data/output-under20")


def main():
    ui = int(input("Choose an example: "))
    match ui:
        case 1:
            count_words()
        case 2:
            count_stream()
        case 3:
            find_spent()
        case 4:
            query_data()
        case _:
            raise "Unsupported.."


if __name__ == "__main__":
    main()
