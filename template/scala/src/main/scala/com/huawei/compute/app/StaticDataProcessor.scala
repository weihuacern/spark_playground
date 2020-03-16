package com.huawei.compute.app

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, collect_list, concat, lit}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

import com.huawei.compute.base.ComputeAppBase

class StaticDataProcessor(connStr: String) extends ComputeAppBase(connStr) {
    def readInputDataframe() : DataFrame = {
        val data = Seq(
            Row("172.16.1.2", 3306,
                "employee", "switzerland", "eid"),
            Row("172.16.1.2", 3306,
                "employee", "switzerland", "name"),
            Row("172.16.1.2", 3306,
                "employee", "switzerland", "phone_number"),
            Row("172.16.1.2", 3306,
                "employee", "switzerland", "gender"),
            Row("172.16.1.2", 3306,
                "employee", "switzerland", "email"),
            Row("172.16.1.2", 3306,
                "employee", "switzerland", "birthday"),
            Row("172.16.1.3", 1433,
                "customer", "china", "cid"),
            Row("172.16.1.3", 1433,
                "customer", "china", "name"),
            Row("172.16.1.3", 1433,
                "customer", "china", "email"),
            Row("172.16.1.3", 1433,
                "customer", "china", "phone_number"),
            Row("172.16.1.3", 1433,
                "customer", "china", "address"),
            Row("172.16.1.4", 5432,
                "employee", "japan", "eid"),
            Row("172.16.1.4", 5432,
                "employee", "japan", "name"),
            Row("172.16.1.4", 5432,
                "employee", "japan", "phone_number"),
            Row("172.16.1.4", 5432,
                "employee", "japan", "gender"),
            Row("172.16.1.4", 5432,
                "employee", "japan", "email"),
            Row("172.16.1.4", 5432,
                "employee", "japan", "birthday")
        );
        
        val schema = List(
            StructField("host", StringType, true),
            StructField("port", IntegerType, true),
            StructField("db_name", StringType, true),
            StructField("tab_name", StringType, true),
            StructField("col_name", StringType, true)
        );
        
        val df = this.sparkSession.createDataFrame(
            this.sparkSession.sparkContext.parallelize(data),
            StructType(schema)
        );
        return df;
    }

    def transformDataframe(dfI: DataFrame) : DataFrame = {
        var dfO = dfI.withColumn("node_id", concat(col("host"), lit(this.delimiter),
                                                   col("port"), lit(this.delimiter),
                                                   col("db_name"), lit(this.delimiter),
                                                   col("tab_name")).as("node_id"))
                     .select("node_id", "col_name")
                     .groupBy("node_id")
                     .agg(collect_list("col_name").as("col_name_list"));
        return dfO;
    }
    
    def writeOutputDataFrame(df: DataFrame) {
        /*
        CREATE TABLE IF NOT EXISTS static_data (
        node_id TEXT NOT NULL,
        col_name_list TEXT[],
        PRIMARY KEY(node_id)
        );
        */
        val connProps = new Properties();
        connProps.put("user", "postgres");
        connProps.put("password", "postgres");
        df.write
        .jdbc("jdbc:postgresql:127.0.0.1:5432", "compute_engine_data.static_data", connProps);
    }
    
    def Run() {
        // Read Input
        var dfI = this.readInputDataframe();
        dfI.show(5, false);
        // Transform
        var dfO = this.transformDataframe(dfI);
        dfO.show(5, false);
        dfO.printSchema();
        // Write Output
        //this.writeOutputDataFrame(dfO);
    }
}
