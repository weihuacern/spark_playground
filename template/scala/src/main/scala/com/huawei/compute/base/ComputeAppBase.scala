package com.huawei.compute.base

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

class ComputeAppBase(connStr: String) {
    protected var appName = this.getClass.getSimpleName;
    protected var sparkContext :SparkContext = _;
    protected var sparkSession :SparkSession = _;
    protected var tsFormat :String = "yyyy-MM-dd/HH:mm:ss.SSS";
    protected val delimiter :String = ":::";

    def getJDBCPostgreSQLConnStr() : String = {
        val host = "127.0.0.1";
        val port = 5432;
        val dbName = "compute_engine_data";
        return "jdbc:postgresql://%s:%d/%s".format(host, port, dbName);
    }

    def getJDBCPostgreSQLSecret() : Properties = {
        var connProps = new Properties();
        connProps.put("user", "postgres");
        connProps.put("password", "postgres");
        return connProps;
    }

    def Init() {
        try {
            val conf = new SparkConf().setAppName(this.appName).setMaster(this.connStr);
            this.sparkContext = new SparkContext(conf);
            this.sparkSession = SparkSession.builder.getOrCreate();
        } catch {
            case _: Throwable =>;
        }
    }
    
    // Declare without implementation
    def Run() {
    }

    def Stop() {
        try {
            this.sparkContext.stop();
        } catch {
            case _: Throwable =>;
        }
    }
}
