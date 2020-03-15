package com.huawei.app

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, concat, lit, to_timestamp, udf}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, TimestampType}

import com.huawei.base.ComputeAppBase

class DynamicDataProcessor(connStr: String) extends ComputeAppBase(connStr) {
    def readInputDataframe() : DataFrame = {
        val data = Seq(
            Row("2020-03-14/00:41:00.663",
                "172.16.3.10", 32245, "172.16.1.2", 3306,
                "SELECT eid, phone_number FROM employee.switzerland;"),
            Row("2020-03-14/00:41:01.236",
                "172.16.4.10", 44849, "172.16.1.3", 1433,
                "SELECT cid, gender, email FROM customer.china;"),
            Row("2020-03-14/00:41:01.568",
                "172.16.3.10", 34467, "172.16.1.4", 5432,
                "SELECT eid, email, birthday FROM employee.japan;")
        );
        
        val schema = List(
            StructField("timestamp", StringType, true),
            StructField("orig_h", StringType, true),
            StructField("orig_p", IntegerType, true),
            StructField("resp_h", StringType, true),
            StructField("resp_p", IntegerType, true),
            StructField("cmd", StringType, true)
        );
        
        val df = spark.createDataFrame(
            this.sparkSession.sparkContext.parallelize(data),
            StructType(schema)
        );
        return df;
    }
    
    // A naive SQL parser...
    def sqlParser: (String => (String, String, Array[String])) = { s => (s.split("FROM ")(1).split("\\.")(0),
                                                                         s.split("FROM ")(1).split("\\.")(1).replaceAll(";$", ""),
                                                                         s.split(" FROM")(0).split("SELECT ")(1).split(",").map(x => x.trim())) }

    def transformDataframe(dfI: DataFrame) : DataFrame = {
        val sqlParserUDF = udf(sqlParser);
        var tmpColName = "newCol";
        var dfO = dfI.withColumn(tmpColName, sqlParserUDF(dfI("cmd")))
                     .select("timestamp", "orig_h", "orig_p", "resp_h", "resp_p", tmpColName+".*")
                     .withColumnRenamed("_1","db_name")
                     .withColumnRenamed("_2","tab_name")
                     .withColumnRenamed("_3","col_name_list")
                     .withColumn("ts", to_timestamp(col("timestamp"), this.tsFormat))
                     .withColumn("node_id", concat(col("resp_h"), lit(this.delimiter),
                                                   col("resp_p"), lit(this.delimiter),
                                                   col("db_name"), lit(this.delimiter),
                                                   col("tab_name")).as("node_id"))
                     .select("ts", "node_id",
                             "orig_h", "orig_p", "col_name_list");
        return dfO;
    }
    
    def writeOutputDataFrame(df: DataFrame) {
        /*
        CREATE TABLE IF NOT EXISTS dynamic_data (
        ts TIMESTAMP NOT NULL,
        node_id TEXT NOT NULL,
        orig_h TEXT,
        orig_p INTEGER,
        col_name_list TEXT[],
        PRIMARY KEY(ts, node_id)
        );
        */
        val connProps = new Properties();
        connProps.put("user", "postgres");
        connProps.put("password", "postgres");
        df.write
        //.option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
        .jdbc("jdbc:postgresql:127.0.0.1:5432", "compute_engine_data.dynamic_data", connProps);
    }
    
    def Run() {
        // Load Input
        var dfI = this.readInputDataframe();
        dfI.show(5, false);
        // Reformat
        var dfO = this.transformDataframe(dfI);
        dfO.show(5, false);
        dfO.printSchema();
        // Write Output
        //this.writeOutputDataFrame(dfO);
    }
    
    def TestPrint() {
        println(this.appName);
        println(this.connStr);
    }
}
