package com.huawei.compute

import com.huawei.compute.app.{StaticDataProcessor, DynamicDataProcessor}

object ComputeApplicationEntrypoint {
    protected val connStr :String = "local[4]";

    def RunStaticDataProcessor() {
        val app = new StaticDataProcessor(this.connStr);
        app.Init();
        app.Run();
        app.Stop();
    }

    def RunDynamicDataProcessor() {
        val app = new DynamicDataProcessor(this.connStr);
        app.Init();
        app.Run();
        app.Stop();
    }
    
    def main(args: Array[String]) {
        //val appName = args(0);
        val appName = "aaa";
        appName match {
            case "StaticDataProcessor" => this.RunStaticDataProcessor();
            case "DynamicDataProcessor" => this.RunDynamicDataProcessor();
            case _  => println("Invalid compute application name: " + appName);
        }
    }
}
