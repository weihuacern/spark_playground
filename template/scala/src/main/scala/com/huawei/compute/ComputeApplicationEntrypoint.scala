package com.huawei.compute

import java.lang.ArrayIndexOutOfBoundsException

import com.huawei.compute.app.{StaticDataProcessor, DynamicDataProcessor, CoreDataGenerator}

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

    def RunCoreDataGenerator() {
        val app = new CoreDataGenerator(this.connStr);
        app.Init();
        app.Run();
        app.Stop();
    }
    
    def main(args: Array[String]) {
        var appName : String = "";
        try {
            appName = args(0);
            println("Processing application: " + appName);
        } catch {
            case a: ArrayIndexOutOfBoundsException => println("Give at least 1 param as application name!");
            return;
        }
        appName match {
            case "StaticDataProcessor" => this.RunStaticDataProcessor();
            case "DynamicDataProcessor" => this.RunDynamicDataProcessor();
            case "CoreDataGenerator" => this.RunCoreDataGenerator();
            case _  => println("Invalid compute application name: " + appName);
        }
    }
}
