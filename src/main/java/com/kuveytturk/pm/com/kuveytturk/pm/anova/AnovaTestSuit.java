package com.kuveytturk.pm.com.kuveytturk.pm.anova;

import com.kuveytturk.pm.com.kuveytturk.pm.regression.FlowStateBean;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static com.kuveytturk.pm.com.kuveytturk.pm.regression.Constants.FLOWSTATEMAP;

public class AnovaTestSuit {

    public static void performTestByUserCode(SparkSession spark,
                                                  int flowId,
                                                  int stateId,
                                                  String strBeginDate,
                                                  String strEndDate,
                                                  String dependentVariableName){

        Dataset<Row> baseDF = null;
        List<AnovaStatsBean> anovaStatsBeanList = new ArrayList<>();
        StringBuilder sqlStringBuilder = new StringBuilder();

        if(stateId == 0 || flowId == 0){

            sqlStringBuilder.append("SELECT flowId, stateId, usercode, statestartdate, stateenddate, workingduration, stateduration, instancerunid, ")
                    .append("to_date(from_unixtime(floor(statestartdate / 1000), 'yyyy-MM-dd HH:mm:ss')) AS FormattedBeginDate, ")
                    .append("to_date(from_unixtime(floor(stateenddate / 1000), 'yyyy-MM-dd HH:mm:ss')) AS FormattedEndDate, ")
                    .append("hour(from_unixtime(floor(statestartdate / 1000), 'yyyy-MM-dd HH:mm:ss')) AS FormattedBeginHour, ")
                    .append("hour(from_unixtime(floor(stateenddate / 1000), 'yyyy-MM-dd HH:mm:ss')) AS FormattedEndHour ")
                    .append("FROM processmining.InstanceUserPerformance ")
                    .append("WHERE (instancerunid IS NOT NULL) AND (workingduration IS NOT NULL) AND (stateduration IS NOT NULL)");

        } else {

            sqlStringBuilder.append("SELECT flowId, stateId, usercode, statestartdate, stateenddate, workingduration, stateduration, instancerunid, ")
                    .append("to_date(from_unixtime(floor(statestartdate / 1000), 'yyyy-MM-dd HH:mm:ss')) AS FormattedBeginDate, ")
                    .append("to_date(from_unixtime(floor(stateenddate / 1000), 'yyyy-MM-dd HH:mm:ss')) AS FormattedEndDate, ")
                    .append("hour(from_unixtime(floor(statestartdate / 1000), 'yyyy-MM-dd HH:mm:ss')) AS FormattedBeginHour, ")
                    .append("hour(from_unixtime(floor(stateenddate / 1000), 'yyyy-MM-dd HH:mm:ss')) AS FormattedEndHour ")
                    .append("FROM processmining.InstanceUserPerformance ")
                    .append("WHERE (instancerunid IS NOT NULL) AND (workingduration IS NOT NULL) AND (stateduration IS NOT NULL) AND (workingduration > 0) AND ")
                    .append("(FlowId = ").append(flowId).append(") AND (stateId = ").append(stateId).append(")");
        }

        baseDF = spark.sql(sqlStringBuilder.toString());
        baseDF.createOrReplaceTempView("baseDF");

        //baseDF.write().option("header", "true").csv("/user/faydemir/SparkOUT/BaseDFout.csv");
        //System.out.println("BaseDF:");
        //baseDF.show();

        Dataset<Row> dfCleanedOutFromOutliers = AnovaUtility.removeOutliers(spark, baseDF, dependentVariableName);
        dfCleanedOutFromOutliers.createOrReplaceTempView("dfCleanedOutFromOutliers");
        System.out.println("Outliers have been removed");
        //dfCleanedOutFromOutliers.show();

        sqlStringBuilder = new StringBuilder();
        sqlStringBuilder
                .append("SELECT flowId, StateId, usercode, COUNT(usercode) as CountOfUSerCodes ")
                .append("FROM dfCleanedOutFromOutliers ")
                //.append("WHERE to_date(from_unixtime(floor(statestartdate / 1000), 'yyyy-MM-dd HH:mm:ss')) ")
                .append("WHERE to_date(from_unixtime(floor(statestartdate / 1000))) ")
                .append("BETWEEN to_date('")
                .append(strBeginDate)
                //.append("','yyyy-MM-dd HH:mm:ss') AND to_date('")
                .append("') AND to_date('")
                .append(strEndDate)
                //.append("','yyyy-MM-dd HH:mm:ss') ")
                .append("') ")
                .append("GROUP BY flowId, StateId, usercode ")
                .append("HAVING CountOfUSerCodes > 30 ");
        System.out.println("\nSQL FOR filtering categories with less than 30 records \n" + sqlStringBuilder.toString() + "\n");
        Dataset<Row> dfFilteredByCount30Barrier= spark.sql(sqlStringBuilder.toString());
        dfFilteredByCount30Barrier.createOrReplaceTempView("dfFilteredByCount30Barrier");
        //System.out.println("Filtered less than 30:");
        //dfFilteredByCount30Barrier.show();

        sqlStringBuilder = new StringBuilder();
        sqlStringBuilder
                .append("SELECT dfCleanedOutFromOutliers.flowId, dfCleanedOutFromOutliers.stateId, ")
                .append("dfCleanedOutFromOutliers.usercode, dfCleanedOutFromOutliers.statestartdate, ")
                .append("dfCleanedOutFromOutliers.stateenddate, ")
                .append("dfCleanedOutFromOutliers.workingduration,  ")
                .append("dfCleanedOutFromOutliers.instancerunid, ")
                .append("dfCleanedOutFromOutliers.formattedbegindate, ")
                .append("dfCleanedOutFromOutliers.formattedenddate, ")
                .append("dfCleanedOutFromOutliers.stateduration  ")
                .append("FROM dfCleanedOutFromOutliers, dfFilteredByCount30Barrier ")
                .append("WHERE dfCleanedOutFromOutliers.flowId = dfFilteredByCount30Barrier.flowId AND ")
                .append("dfCleanedOutFromOutliers.stateId = dfFilteredByCount30Barrier.stateId AND ")
                .append("dfCleanedOutFromOutliers.usercode = dfFilteredByCount30Barrier.usercode");
        Dataset<Row> anovaReadyDF= spark.sql(sqlStringBuilder.toString());
        anovaReadyDF.createOrReplaceTempView("anovaReadyDF");
        System.out.println("Before Anova Computation:");
        //anovaReadyDF.show();



        sqlStringBuilder = new StringBuilder();
        sqlStringBuilder
                    .append("SELECT o.usercode AS cat, cast((")
                    .append(dependentVariableName)
                    .append(") AS double) AS value FROM anovaReadyDF o");

        Dataset<Row> oneWayAnovaCategoryValueDF = spark.sql(sqlStringBuilder.toString());
        //System.out.println("Dependent/Independent Column List:");
        //oneWayAnovaCategoryValueDF.show();

        AnovaStatsBean anovaStatsBean = AnovaUtility.computeAnovaStats(
                    spark,
                    oneWayAnovaCategoryValueDF,
                    flowId,
                    stateId,
                    "UserCode",
                    dependentVariableName,
                    strBeginDate,
                    strEndDate);

        if(anovaStatsBean != null) {
           anovaStatsBeanList.add(anovaStatsBean);
        }

        if(!anovaStatsBeanList.isEmpty()) {
            AnovaUtility.saveAnovaStats(spark, anovaStatsBeanList);
        } else {
            System.out.println("anovaStatsBeanList is EMPTY");
        }
    }

    public static void doAnovaTest(SparkSession spark, int flowId, int stateId){
        System.out.println("###########################################");
        System.out.println("#      BEGIN FLOWID:STATEID - " + flowId + ":" + stateId + "       #");
        System.out.println("###########################################");
        String strEndDate = "2017-10-30";
        String strBegin10DaysAgoDate = "2017-09-20";
        String strBegin1MonthAgoDate = "2017-09-30";
        String strBegin3MonthAgoDate = "2017-07-30";
        String strBegin5MonthAgoDate = "2017-06-01";

        performTestByUserCode(spark, flowId, stateId, strBegin3MonthAgoDate, strEndDate, "workingduration");
        System.out.println("###########################################");
        System.out.println("#      END FLOWID:STATEID - " + flowId + ":" + stateId + "     #");
        System.out.println("###########################################");
    }

    public static void performOneWayAnovaTests(SparkSession spark){
        for (FlowStateBean element : FLOWSTATEMAP) {
            int flowId = element.getFlowId();
            int stateId = element.getStateId();
            doAnovaTest(spark, flowId, stateId);
        }
    }


    //        "SELECT flowId, stateId, usercode, statestartdate, stateenddate, instancerunid, workingduration, " +
//                "to_date(from_unixtime(floor(statestartdate / 1000), 'yyyy-MM-dd hh:mm:ss')) AS FormattedBeginDate, " +
//                "to_date(from_unixtime(floor(stateenddate / 1000), 'yyyy-MM-dd hh:mm:ss')) AS FormattedEndDate " +
//                "FROM processmining.InstanceUserPerformance WHERE (instancerunid IS NOT NULL) AND (workingduration IS NOT NULL)");
}
