package com.kuveytturk.pm.com.kuveytturk.pm.regression;

import com.kuveytturk.pm.com.kuveytturk.pm.anova.AnovaUtility;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

import static com.kuveytturk.pm.com.kuveytturk.pm.regression.Constants.FLOWSTATEMAP;
import static org.apache.spark.sql.functions.*;

public class LinearRegression {
    public static List<LROutcome> outcomeList= new ArrayList<>();
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HHmmss");

    public static void doLinearRegression(SparkSession spark,
                                     int flowId,
                                     int stateId,
                                     String strBeginDate,
                                     String strEndDate,
                                     String yLabel,
                                     double significanceValue,
                                     String period) {

        StringBuilder sqlStringBuilder = new StringBuilder();

        sqlStringBuilder
                .append("SELECT flowId, stateid, usercode, actionid, instancerunid, stateduration, workingduration, poolduration, inboxduration, statestartdate, stateenddate, startdate, enddate ")
                .append("FROM processmining.InstanceUserPerformance ")
                .append("WHERE (instancerunid IS NOT NULL) AND (workingduration IS NOT NULL) AND ")
                .append("(stateduration IS NOT NULL) AND (poolduration IS NOT NULL) AND (inboxduration IS NOT NULL) AND ")
                .append("(stateduration > 0) AND ")
                .append("(statestartdate IS NOT NULL) AND (stateenddate IS NOT NULL) AND ")
                .append("to_date(from_unixtime(floor(statestartdate / 1000), 'yyyy-MM-dd HH:mm:ss')) ")
                .append("BETWEEN to_date('")
                .append(strBeginDate)
                .append("') AND to_date('")
                .append(strEndDate)
                .append("') ")
                .append(" AND flowId = ").append(flowId).append(" AND stateid = ").append(stateId)
                .append(" ORDER BY InstanceRunId ASC ");

        Dataset<Row> rawDF = spark.sql(sqlStringBuilder.toString());
        if(rawDF.count() == 0){
            System.out.println("rawDF is EMPTY");
            return;
        }
        System.out.println("rawDF: ");
        //rawDF.show();

        Dataset<Row> noOutlierDF = AnovaUtility.removeOutliers(spark, rawDF, yLabel);
        noOutlierDF.createOrReplaceTempView("noOutlierDF");
        if(noOutlierDF.count() == 0){
            System.out.println("noOutlierDF is EMPTY");
            return;
        }

        Dataset<Row> lookupDF = spark.sql("SELECT FlowId, StateId, COUNT(StateId) as countNum FROM noOutlierDF GROUP BY FlowId, StateId HAVING countNum >= 30");
        lookupDF.createOrReplaceTempView("lookupDF");
        System.out.println("lookupDF: ");
        //lookupDF.show();

        sqlStringBuilder = new StringBuilder();
        sqlStringBuilder
                .append("SELECT noOutlierDF.InstanceRunId, noOutlierDF.StateStartDate, noOutlierDF.").append(yLabel)
                .append(" FROM lookupDF, noOutlierDF ")
                .append("WHERE lookupDF.FlowId = noOutlierDF.FlowId AND lookupDF.StateId = noOutlierDF.StateId ORDER BY noOutlierDF.InstanceRunId ");
        Dataset<Row> baseDF= spark.sql(sqlStringBuilder.toString());
        baseDF.createOrReplaceTempView("baseDF");
        System.out.println("baseDF: ");
        //baseDF.show();
        if(baseDF.count() == 0){
            System.out.println("baseDF is EMPTY");
            return;
        }

        String sqlStr = String.format("SELECT row_number() over (order by InstanceRunId) as x, %s AS y FROM baseDF", yLabel);//String.format("SELECT log(10,statestartdate) as x, %s AS y FROM baseDF", yLabel);//String.format("SELECT row_number() over (order by InstanceRunId) as x, %s AS y FROM baseDF", yLabel);
        Dataset<Row> xyDF =  spark.sql(sqlStr);
        xyDF.createOrReplaceTempView("xyDF");
        long rowCount = xyDF.count();
        System.out.println("xyDF COUNT: " + rowCount);
        System.out.println("xyDF: ");
        //xyDF.show();
        if(baseDF.count() == 0){
            System.out.println("baseDF is EMPTY");
            return;
        }

        double avgX = toDouble(xyDF.select(mean(col("x"))).first().get(0));
        System.out.println("avgX: " + avgX);

        sqlStringBuilder = new StringBuilder();
        Formatter fmt = new Formatter(sqlStringBuilder);
        fmt.format("SELECT  (sum(x * y) - ((sum(y) * sum(x))/%d))/(sum((x - %f) * (x - %f))) FROM xyDF GROUP BY x, y", rowCount, avgX, avgX);
        Dataset<Row> beta1DF =  spark.sql(sqlStringBuilder.toString());
        double beta1Hat = toDouble(beta1DF.first().get(0));
        System.out.println("beta1Hat: " + beta1Hat);

        sqlStringBuilder = new StringBuilder();
        fmt = new Formatter(sqlStringBuilder);
        fmt.format("SELECT  (avg(y) - (%f * %f)) FROM xyDF", beta1Hat, avgX);
        Dataset<Row> beta0DF =  spark.sql(sqlStringBuilder.toString());
        double beta0Hat = toDouble(beta0DF.first().get(0));
        System.out.println("beta0Hat: " + beta0Hat);

        sqlStringBuilder = new StringBuilder();
        fmt = new Formatter(sqlStringBuilder);
        fmt.format("SELECT (%f + (%f * x)) AS yHat, x, y FROM xyDF GROUP BY x, y", beta0Hat, beta1Hat);
        Dataset<Row> yHatDF =  spark.sql(sqlStringBuilder.toString());
        yHatDF.createOrReplaceTempView("yHatDF");
        System.out.println("yHatDF: ");
        //yHatDF.show();

        sqlStringBuilder = new StringBuilder();
        fmt = new Formatter(sqlStringBuilder);
        fmt.format("SELECT x, y, (y - yHat) AS e FROM yHatDF GROUP BY x, y, yHat", beta0Hat, beta1Hat);
        Dataset<Row> eDF =  spark.sql(sqlStringBuilder.toString());
        eDF.createOrReplaceTempView("eDF");
        System.out.println("eDF: ");
        //eDF.show();

        long degreesOfFreedom = rowCount - 2;
        System.out.println("degreesOfFreedom: " + degreesOfFreedom);

        sqlStringBuilder = new StringBuilder();
        fmt = new Formatter(sqlStringBuilder);
        fmt.format("SELECT (sum((e * e)/(%d)))/(sum((x - %f)*(x - %f))) as s FROM eDF GROUP BY e, x", degreesOfFreedom, avgX, avgX);
        Dataset<Row> seBeta1DF = spark.sql(sqlStringBuilder.toString());
        System.out.println("seBeta1DF: ");
        //seBeta1DF.show();

        double seBeta1 = toDouble(seBeta1DF.select(sqrt(col("s"))).first().get(0));
        System.out.println("seBeta1: " + seBeta1);
        double beta10  = 0;
        double T0 = (beta1Hat - beta10)/ seBeta1;
        System.out.println("T0: " + T0);

        //TDistribution(double degreesOfFreedom, double inverseCumAccuracy)
        TDistribution tdist = new TDistribution(degreesOfFreedom, significanceValue);
        double probOfT0 = tdist.cumulativeProbability(T0);
        System.out.println("probOfT0: " + probOfT0);

        double pValue = 2 * (1 - probOfT0);
        System.out.println("pValue: " + pValue);

        int timeChangeSignificance =   (pValue > significanceValue ? 0 : 1);
        int meaning =   (pValue <= significanceValue ? 0 : 1);

        LROutcome outcome = new LROutcome(
                        yLabel,
                        strBeginDate + "~" + strEndDate,
                        period,
                        pValue,
                        T0,
                        beta1Hat,
                        beta0Hat,
                        significanceValue,
                        stateId,
                        flowId,
                        timeChangeSignificance,
                        meaning );

        outcomeList.add(outcome);
    }

    public static void doTTest(SparkSession spark, int flowId, int stateId){
        System.out.println("###########################################");
        System.out.println("#      BEGIN FLOWID:STATEID - " + flowId + ":" + stateId + "       #");
        System.out.println("###########################################");
        String strEndDate = "2017-10-30";
        String strBegin10DaysAgoDate = "2017-09-20";
        String strBegin1MonthAgoDate = "2017-09-30";
        String strBegin3MonthAgoDate = "2017-07-30";
        String strBegin5MonthAgoDate = "2017-06-01";
        //doLinearRegression(spark, flowId, stateId, strBegin10DaysAgoDate, strEndDate, "StateDuration", 0.05d, "Last10Days");
        doLinearRegression(spark, flowId, stateId, strBegin1MonthAgoDate, strEndDate, "StateDuration", 0.05d, "Last1Month");
        doLinearRegression(spark, flowId, stateId, strBegin3MonthAgoDate, strEndDate, "StateDuration", 0.05d, "Last3Months");
        //doLinearRegression(spark, flowId, stateId, strBegin5MonthAgoDate, strEndDate, "StateDuration", 0.05d, "Last5Months");

        System.out.println("Outcome List Size: " + outcomeList.size());
        if(outcomeList.size() > 0) {
            Dataset<Row> recordsDF = spark.createDataFrame(outcomeList, LROutcome.class);
            System.out.println("OutcomeDf: ");
            recordsDF.show();
            recordsDF.write().option("header", "true").csv("/user/faydemir/SparkOUT2/LR2/" + sdf.format(java.util.Calendar.getInstance().getTime()) + "_" + flowId + "_" + stateId + ".csv");
            outcomeList.clear();
        } else {
            System.out.println("Outcome List has no items!");
        }

        System.out.println("###########################################");
        System.out.println("#      END FLOWID:STATEID - " + flowId + ":" + stateId + "     #");
        System.out.println("###########################################");
    }

    public static void performTTests(SparkSession spark){
        for (FlowStateBean element : FLOWSTATEMAP) {
            int flowId = element.getFlowId();
            int stateId = element.getStateId();
            doTTest(spark, flowId, stateId);
        }
    }

    public static void saveBaseData(SparkSession spark) {
        StringBuilder sqlStringBuilder = new StringBuilder();

        String strEndDate = "2017-08-02";
        String strBeginDate = "2017-08-01";

        sqlStringBuilder
                .append("SELECT flowId, stateid, usercode, actionid, instancerunid, stateduration, workingduration, poolduration, inboxduration, statestartdate, stateenddate, startdate, enddate ")
                .append("FROM processmining.InstanceUserPerformance ")
                .append("WHERE (instancerunid IS NOT NULL) AND (workingduration IS NOT NULL) AND ")
                .append("(stateduration IS NOT NULL) AND (poolduration IS NOT NULL) AND (inboxduration IS NOT NULL) AND ")
                .append("(statestartdate IS NOT NULL) AND (stateenddate IS NOT NULL) ")
                .append("ORDER BY InstanceRunId ASC ");

        Dataset<Row> baseDF = spark.sql(sqlStringBuilder.toString());
        baseDF.createOrReplaceTempView("baseDF");
        baseDF.write().option("header", "true").csv("/user/faydemir/SparkOUT2/Base/AllInOne.csv");
        baseDF.show();
    }

    private static double toDouble(Object value){
        double retVal = 0d;
        if(value instanceof  Double){
            retVal = ((Double) value).doubleValue();
        } else if (value instanceof Long){
            retVal = ((Long) value).doubleValue();
        } else if (value == null){
            retVal = 0d;
        }
        return retVal;
    }

    private static long toLong(Object value){
        long retVal = 0;
        if(value instanceof  Long){
            retVal = ((Long) value).longValue();
        }if(value instanceof  Integer){
            retVal = ((Long) value).longValue();
        } else if (value == null){
            retVal = 0;
        }
        return retVal;
    }

}
