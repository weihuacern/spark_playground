package com.huawei.base

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

class ComputeAppBase(connStr: String) {
    protected var appName = this.getClass.getSimpleName;
    protected var sparkContext :SparkContext = _;
    protected var sparkSession :SparkSession = _;
    protected var tsFormat :String = "yyyy-MM-dd/HH:mm:ss.SSS";
    protected val delimiter :String = ":::";

    def Init() {
        try {
            val conf = new SparkConf().setAppName(this.appName).setMaster(this.connStr);
            this.sparkContext = new SparkContext(conf);
            this.sparkSession = SparkSession.builder.getOrCreate();
        } catch {
            case _: Throwable =>;
        }
    }
    
    def Stop() {
        try {
            this.sparkContext.stop();
        } catch {
            case _: Throwable =>;
        }
    }
}
