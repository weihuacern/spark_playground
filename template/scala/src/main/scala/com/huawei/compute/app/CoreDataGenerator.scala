package com.huawei.compute.app

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions.{col, concat}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, TimestampType}

import com.huawei.compute.base.ComputeAppBase

class CoreDataGenerator(connStr: String) extends ComputeAppBase(connStr) {
    def readInputDataframe() : (DataFrame, DataFrame) = {
        val jdbcPostgreSQLConnStr = this.getJDBCPostgreSQLConnStr();
        var connProps = this.getJDBCPostgreSQLSecret();
        val dfStatic = this.sparkSession.read.jdbc(jdbcPostgreSQLConnStr, "static_data", connProps);
        val dfDynamic = this.sparkSession.read.jdbc(jdbcPostgreSQLConnStr, "dynamic_data", connProps);
        return (dfStatic, dfDynamic);
    }

    def transformDataframe(dfStatic: DataFrame, dfDynamic: DataFrame) : DataFrame = {
        // TODO, To be implemented
        var dfO = dfDynamic;
        return dfO;
    }
    
    def writeOutputDataFrame(df: DataFrame) {
        // TODO, To be implemented
        val jdbcPostgreSQLConnStr = this.getJDBCPostgreSQLConnStr();
        var connProps = this.getJDBCPostgreSQLSecret();

        df.write
        .mode(SaveMode.Overwrite)
        .jdbc(jdbcPostgreSQLConnStr, "xxx", connProps);
    }
    
    override def Run() {
        // Load Input
        var (dfStatic, dfDynamic) = this.readInputDataframe();
        dfStatic.show(5, false);
        dfDynamic.show(5, false);
        // Reformat
        var dfO = this.transformDataframe(dfStatic, dfDynamic);
        dfO.show(5, false);
        dfO.printSchema();
        // Write Output
        //this.writeOutputDataFrame(dfO);
    }
}
