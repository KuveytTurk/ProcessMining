package com.kuveytturk.pm.com.kuveytturk.pm.anova;

import java.text.SimpleDateFormat;
import java.util.Objects;

public class AnovaStatsBean implements java.io.Serializable{

    private int flowId, stateId;
    private String userCode, dependentVariable, independentVariable, oneWayAnovaResult, computationInterval, computationDate, freeText;
    private double dfb, dfw, fValue, pValue;

    public AnovaStatsBean(){
        flowId = 0;
        stateId = 0;
        userCode = "";
        dependentVariable = "";
        independentVariable = "";
        oneWayAnovaResult = "";
        computationInterval = "";
        computationDate = "";
        freeText = "";

        dfb = 0d;
        dfw = 0d;;
        fValue = 0d;;
        pValue = 0d;;
    }

    public AnovaStatsBean(int flowId, int stateId, String dependentVariable, String independentVariable, String oneWayAnovaResult, String computationInterval, double dfb, double dfw, double fValue, double pValue) {
        this.flowId = flowId;
        this.stateId = stateId;
        this.userCode = userCode;
        this.dependentVariable = dependentVariable;
        this.independentVariable = independentVariable;
        this.oneWayAnovaResult = oneWayAnovaResult;
        this.computationInterval = computationInterval;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        computationDate = sdf.format(java.util.Calendar.getInstance(java.util.TimeZone.getDefault()).getTime());
        this.freeText = freeText;
        this.dfb = dfb;
        this.dfw = dfw;
        this.fValue = fValue;
        this.pValue = pValue;
    }

    public int getFlowId() {
        return flowId;
    }

    public void setFlowId(int flowId) {
        this.flowId = flowId;
    }

    public int getStateId() {
        return stateId;
    }

    public void setStateId(int stateId) {
        this.stateId = stateId;
    }

    public String getUserCode() {
        return userCode;
    }

    public void setUserCode(String userCode) {
        this.userCode = userCode;
    }

    public String getDependentVariable() {
        return dependentVariable;
    }

    public void setDependentVariable(String dependentVariable) {
        this.dependentVariable = dependentVariable;
    }

    public String getIndependentVariable() {
        return independentVariable;
    }

    public void setIndependentVariable(String independentVariable) {
        this.independentVariable = independentVariable;
    }

    public String getOneWayAnovaResult() {
        return oneWayAnovaResult;
    }

    public void setOneWayAnovaResult(String oneWayAnovaResult) {
        this.oneWayAnovaResult = oneWayAnovaResult;
    }

    public String getComputationInterval() {
        return computationInterval;
    }

    public void setComputationInterval(String computationInterval) {
        this.computationInterval = computationInterval;
    }

    public String getComputationDate() {
        return computationDate;
    }

    public void setComputationDate(String computationDate) {
        this.computationDate = computationDate;
    }

    public String getFreeText() {
        return freeText;
    }

    public void setFreeText(String freeText) {
        this.freeText = freeText;
    }

    public double getDfb() {
        return dfb;
    }

    public void setDfb(double dfb) {
        this.dfb = dfb;
    }

    public double getDfw() {
        return dfw;
    }

    public void setDfw(double dfw) {
        this.dfw = dfw;
    }

    public double getfValue() {
        return fValue;
    }

    public void setfValue(double fValue) {
        this.fValue = fValue;
    }

    public double getpValue() {
        return pValue;
    }

    public void setpValue(double pValue) {
        this.pValue = pValue;
    }

    @Override
    public String toString() {
        return "AnovaStatsBean{" +
                "flowId=" + flowId +
                ", stateId=" + stateId +
                ", userCode='" + userCode + '\'' +
                ", dependentVariable='" + dependentVariable + '\'' +
                ", independentVariable='" + independentVariable + '\'' +
                ", oneWayAnovaResult='" + oneWayAnovaResult + '\'' +
                ", computationInterval='" + computationInterval + '\'' +
                ", computationDate='" + computationDate + '\'' +
                ", freeText='" + freeText + '\'' +
                ", dfb=" + dfb +
                ", dfw=" + dfw +
                ", fValue=" + fValue +
                ", pValue=" + pValue +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnovaStatsBean that = (AnovaStatsBean) o;
        return flowId == that.flowId &&
                stateId == that.stateId &&
                Double.compare(that.dfb, dfb) == 0 &&
                Double.compare(that.dfw, dfw) == 0 &&
                Double.compare(that.fValue, fValue) == 0 &&
                Double.compare(that.pValue, pValue) == 0 &&
                Objects.equals(userCode, that.userCode) &&
                dependentVariable.equals(that.dependentVariable) &&
                independentVariable.equals(that.independentVariable) &&
                oneWayAnovaResult.equals(that.oneWayAnovaResult) &&
                computationInterval.equals(that.computationInterval) &&
                computationDate.equals(that.computationDate) &&
                Objects.equals(freeText, that.freeText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flowId, stateId, userCode, dependentVariable, independentVariable, oneWayAnovaResult, computationInterval, computationDate, freeText, dfb, dfw, fValue, pValue);
    }
}
