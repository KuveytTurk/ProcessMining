package com.kuveytturk.pm;

import com.kuveytturk.pm.com.kuveytturk.pm.anova.AnovaTestSuit;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main implements java.io.Serializable{

    public static void main(String[] args){
        Logger LOG = LoggerFactory.getLogger("com.kuveytturk.pm");
        SparkSession spark = SparkSession.builder().appName("KTProcessMining").config(new SparkConf()).getOrCreate();
        SparkContext sc = spark.sparkContext();
        sc.setLogLevel("WARN");
        AnovaTestSuit.performOneWayAnovaTests(spark);
    }
}