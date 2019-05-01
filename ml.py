########################################################################################################
########################################################################################################
#########################################Sorteerstraatjes###############################################
########################################################################################################
########################################################################################################

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ml-sorteer2').getOrCreate()

df1 = spark.read.csv('ml_input.csv', header = True, inferSchema = True)
df1
df1.printSchema()

import pandas as pd
import matplotlib

pd.DataFrame(df1.take(5), columns=df1.columns).transpose()

numeric_features = [t[0] for t in df1.dtypes if t[1] == 'int']
df1.select(numeric_features).describe().toPandas().transpose()

        from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler

        categoricalColumns = ['lediging_24h_later']
        stages1 = []
        for categoricalCol in categoricalColumns:
            stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + 'Index')
            encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()],
                                             outputCols=[categoricalCol + "classVec"])
            stages1 += [stringIndexer, encoder]
        label_stringIdx = StringIndexer(inputCol='lediging_24h_later', outputCol='label')
        stages1 += [label_stringIdx]
        numericCols = ['telling_sinds_lediging', 'telling']
        assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
        assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
        stages1 += [assembler]

        from pyspark.ml import Pipeline

        cols = df1.columns

        pipeline2 = Pipeline(stages=stages1)
        pipelineModel = pipeline2.fit(df1)
        df1 = pipelineModel.transform(df1)
        selectedCols = ['label', 'features'] + cols
        df1 = df1.select(selectedCols)
        df1.printSchema()

        pd.DataFrame(df1.take(5), columns=df1.columns).transpose()

        train, test = df1.randomSplit([0.9, 0.1], seed=2018)
        print("Training Dataset Count: " + str(train.count()))
        print("Test Dataset Count: " + str(test.count()))

        from pyspark.ml.classification import LogisticRegression

        lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10)
        lrModel = lr.fit(train)

        import matplotlib.pyplot as plt
        import numpy as np

        beta = np.sort(lrModel.coefficients)
        plt.plot(beta)
        plt.ylabel('Beta Coefficients')
        plt.show()

        trainingSummary = lrModel.summary
        roc = trainingSummary.roc.toPandas()
        plt.plot(roc['FPR'], roc['TPR'])
        plt.ylabel('False Positive Rate')
        plt.xlabel('True Positive Rate')
        plt.title('ROC Curve')
        plt.show()
        print('Training set areaUnderROC: ' + str(trainingSummary.areaUnderROC))

        pr = trainingSummary.pr.toPandas()
        plt.plot(pr['recall'], pr['precision'])
        plt.ylabel('Precision')
        plt.xlabel('Recall')
        plt.show()

        predictions = lrModel.transform(test)
        predictions.select('telling_sinds_lediging', 'telling', 'label', 'rawPrediction', 'prediction', 'probability').show(1000)

        from pyspark.ml.evaluation import BinaryClassificationEvaluator

        evaluator = BinaryClassificationEvaluator()
        print('Test Area Under ROC', evaluator.evaluate(predictions))



        # using SQLContext to read parquet file
        from pyspark.sql import SQLContext

        sqlContext = SQLContext(sc)  ##wanneer de mapping/ETL doen? on-the-fly

        # to read parquet file
        df = sqlContext.read.parquet('test.parquet')

        print(df)

        df.printSchema()
        df.show()
        df.columns
        df.summary()

        # postgresql connectie - direct
        import pandas as pd
        import psycopg2

        connection = psycopg2.connect("dbname= db_gis user=postgres password=Antwerpen1713")
        cursor = connection.cursor()
        cursor.execute("select * from container_activiteit limit 10000")
        df = cursor.fetchall()
        print(df)

    pd.DataFrame(df)

    from pyspark.sql import DataFrameReader
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    from pyspark import SparkConf

    import pyspark.sql

    conf = SparkConf()
    Class.forName("com.mysql.jdbc.Driver")
    conf.set("spark.jars", )
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = SQLContext(sc)
    url = 'postgresql://localhost:5432/db_gis'
    properties = {'user': 'postgres', 'password': 'Antwerpen1713'}
    df = DataFrameReader(sqlContext).jdbc(
        url='jdbc:%s' % url, table='container_activiteit', properties=properties
    )

    df.write.jdbc(url="jdbc:mysql://localhost:5432/db_gis"
                      "?user=postgres&password=Antwerpen1713",
                  table="container_activiteit",
                  mode="append",
                  properties={"driver": 'com.mysql.jdbc.Driver'})

    from pyspark.sql import SparkSession

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config(conf=conf) \
        .getOrCreate()


    def jdbc_dataset_example(spark):
        # $example on:jdbc_dataset$
        # Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
        # Loading data from a JDBC source
        jdbcDF = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql:db_gis") \
            .option("dbtable", "schema.container_activiteit") \
            .option("user", "postgres") \
            .option("password", "Antwerpen1713") \
            .load()

        jdbcDF2 = spark.read \
            .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
                  properties={"user": "username", "password": "password"})


    spark = SparkSession.builder \
        .config(conf=conf) \
        .master("local") \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()

    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:db_gis") \
        .option("dbtable", "container_activiteit") \
        .option("user", "postgres") \
        .option("password", "Antwerpen1713") \
        .load()

    jdbcDF2 = spark.read \
        .jdbc("jdbc:postgresql:db_gis", "container_activiteit",
              properties={"user": "postgres", "password": "Antwerpen1713"})

    from pyspark.sql import DataFrameReader

    from pyspark import spark.read

    url = 'postgresql://localhost:5432/db_gis'
    properties = {'user': 'postgres', 'password': 'Antwerpen1713'}
    df = DataFrameReader(sqlContext).jdbc(
        url='jdbc:%s' % url, table='container_activiteit', properties=properties
    )

