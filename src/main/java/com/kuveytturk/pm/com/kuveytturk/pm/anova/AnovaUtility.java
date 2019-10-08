package com.kuveytturk.pm.com.kuveytturk.pm.anova;

import org.apache.commons.math3.distribution.FDistribution;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Formatter;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class AnovaUtility {

    public static AnovaStatsBean computeAnovaStats(
            SparkSession spark,
            Dataset<Row> anovaBaseDF,
            int flowId,
            int stateId,
            String performTestByUserCodeName,
            String dependentVariable,
            String testStartDate,
            String testEndDate){

        anovaBaseDF.createOrReplaceTempView("anovaBaseDF");
        double N = toDouble(anovaBaseDF.count());
        if(N == 0d){
           return null;
        }

        Dataset<Row> computationBaseDF = spark.sql("SELECT cat, avg(value) as valueavg, count(value) as samplesize FROM anovaBaseDF GROUP BY cat");
        computationBaseDF.createOrReplaceTempView("computationBaseDF");

        double grandMean = toDouble(spark.sql("SELECT avg(value) FROM anovaBaseDF").first().get(0));
        long k = anovaBaseDF.select(col("cat")).distinct().count();
        double dfBetween = Double.valueOf(k) - 1d;


        Dataset<Row> ssBetweenDF =
                spark.sql("SELECT computationBaseDF.cat, computationBaseDF.valueavg, computationBaseDF.samplesize " +
                                 "FROM computationBaseDF ");
        ssBetweenDF.createOrReplaceTempView("ssBetweenDF");

        StringBuilder sqlStringBuilder = new StringBuilder();
        Formatter fmt = new Formatter(sqlStringBuilder);
        fmt.format("SELECT cast(sum((samplesize * ((valueavg - %f) * (valueavg - %f)))) as double) FROM ssBetweenDF",
                grandMean, grandMean);
        double msBetween = toDouble(spark.sql(sqlStringBuilder.toString()).first().get(0)) / dfBetween;

        double dfWithin = N - Double.valueOf(k);

        Dataset<Row> ssWithinGroupDF = spark.sql(
                "SELECT ((sum((anovaBaseDF.value - computationBaseDF.valueavg) * " +
                "(anovaBaseDF.value - computationBaseDF.valueavg)))) " +
                "FROM anovaBaseDF, computationBaseDF  " +
                "WHERE anovaBaseDF.cat = computationBaseDF.cat");
        double msWithin = toDouble(ssWithinGroupDF.first().get(0)) / (N-k);

        double fValue = msBetween / msWithin;
        double pValue = 0d;

        try {
            FDistribution fDist = new FDistribution(dfBetween, dfWithin, 0.05);
            pValue = 1d - fDist.cumulativeProbability(fValue);
        } catch(Exception ex){
            System.out.println("Exception occured while getting fStatsValue: " + ex.getMessage());
            AnovaStatsBean anovaStats = new AnovaStatsBean();
            anovaStats.setOneWayAnovaResult("Anova calculation failed!");
            return anovaStats;
        }

        System.out.println("N: " + N);
        System.out.println("k: " + k);
        System.out.println("grandMean: " + grandMean);
        System.out.println("dfBetween: " + dfBetween);
        System.out.println("MsBetween: " + msBetween);
        System.out.println("dfWthin: " + dfWithin);
        System.out.println("MsWithin: " + msWithin);
        System.out.println("FValue: " + fValue);
        System.out.println("PValue: " + pValue);
        System.out.println("AlfaValue: " + 0.05d);
        System.out.println("Are Groups different: " + ((pValue < 0.05d) ? "1" : "0"));

        AnovaStatsBean anovaStats = new AnovaStatsBean(
                flowId,
                stateId,
                dependentVariable,
                performTestByUserCodeName,
                (pValue < 0.05d) ? "1" : "0",
                testStartDate + "~" + testEndDate,
                dfBetween,
                dfWithin,
                fValue,
                pValue
                );

        System.out.println("\nAnovaBeanV2 Content is: " + anovaStats.toString());

        return anovaStats;
    }

    public static Dataset<Row> removeOutliers(SparkSession spark, Dataset<Row> outlierBaseDF, String columnNameForFiltering){
        StringBuilder sqlStringBuilder = new StringBuilder();
        double[] probs = {0.25, 0.75};
        double[] quantiles = outlierBaseDF.stat().approxQuantile(columnNameForFiltering, probs, 0.05d);

        if(quantiles == null || quantiles.length == 0){
            return outlierBaseDF;
        }

        double Q1 = quantiles[0];
        double Q3 = quantiles[1];
        double IQR = Q3 -Q1;
        double lowerRange = Q1 - 1.5 * IQR;
        double upperRange = Q3 + 1.5 * IQR;

        outlierBaseDF.createOrReplaceTempView("outlierBaseDF");

        sqlStringBuilder.append("SELECT * FROM outlierBaseDF WHERE ")
        .append(columnNameForFiltering)
        .append(" BETWEEN ")
        .append(lowerRange)
        .append(" AND ")
        .append(upperRange);

        Dataset<Row> dfInBetweenOutliers = spark.sql(sqlStringBuilder.toString());
        System.out.println("Q1: " + Q1 + "\nQ3: " + Q3 + "\nIQR: " + IQR);
        System.out.println(quantiles == null ? "QUANTILES is null" : "OUTLIER LowerRange - UpperRange: " + lowerRange + "-" + upperRange);

        return dfInBetweenOutliers;
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

    private static int toInteger(Object value){
        int retVal = 0;
        if(value instanceof  Integer){
            retVal = ((Integer) value).intValue();
        } else if (value == null){
            retVal = 0;
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

    public static void saveAnovaStats(SparkSession spark, List<AnovaStatsBean> anovaStatsBeanList)
    {
        StringBuilder sqlStringBuilder = new StringBuilder();
        Dataset<Row> recordsDF = spark.createDataFrame(anovaStatsBeanList, AnovaStatsBean.class);
        recordsDF.createOrReplaceTempView("recordsToSaveDT");

        sqlStringBuilder
                .append("INSERT INTO TABLE ProcessMining.OneWayAnovaResults " )
        .append("SELECT flowId, stateId, usercode, dependentvariable, independentvariable, ")
        .append("dfb, dfw, fvalue, pvalue, onewayanovaresult, computationinterval, ")
        .append("computationdate, freetext ")
        .append("FROM recordsToSaveDT");

        spark.sql(sqlStringBuilder.toString());
        recordsDF.show();
    }
}
