package com.kuveytturk.pm.com.kuveytturk.pm.regression;

import java.text.SimpleDateFormat;
import java.util.Objects;

public class LROutcome implements java.io.Serializable{
    private String independentVariable, dateInterval, currentDate, period;
    private double pValue, tValue, beta1, beta0, significanceValue;
    private int stateId, flowId, timeChangeSignificance, meaning;

    public LROutcome() { }

    public LROutcome(String independentVariable,
                     String dateInterval,
                     String period,
                     double pValue,
                     double tValue,
                     double beta1,
                     double beta0,
                     double significanceValue,
                     int stateId,
                     int flowId,
                     int timeChangeSignificance,
                     int meaning) {
        this.independentVariable = independentVariable;
        this.dateInterval = dateInterval;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.currentDate = sdf.format(java.util.Calendar.getInstance().getTime());

        this.period = period;
        this.pValue = pValue;
        this.tValue = tValue;
        this.beta1 = beta1;
        this.beta0 = beta0;
        this.significanceValue = significanceValue;
        this.stateId = stateId;
        this.flowId = flowId;
        this.timeChangeSignificance = timeChangeSignificance;
        this.meaning = meaning;
    }

    public String getIndependentVariable() {
        return independentVariable;
    }

    public void setIndependentVariable(String independentVariable) {
        this.independentVariable = independentVariable;
    }

    public String getDateInterval() {
        return dateInterval;
    }

    public void setDateInterval(String dateInterval) {
        this.dateInterval = dateInterval;
    }

    public String getCurrentDate() {
        return currentDate;
    }

    public void setCurrentDate(String currentDate) {
        this.currentDate = currentDate;
    }

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }

    public double getpValue() {
        return pValue;
    }

    public void setpValue(double pValue) {
        this.pValue = pValue;
    }

    public double gettValue() {
        return tValue;
    }

    public void settValue(double tValue) {
        this.tValue = tValue;
    }

    public double getBeta1() {
        return beta1;
    }

    public void setBeta1(double beta1) {
        this.beta1 = beta1;
    }

    public double getBeta0() {
        return beta0;
    }

    public void setBeta0(double beta0) {
        this.beta0 = beta0;
    }

    public double getSignificanceValue() {
        return significanceValue;
    }

    public void setSignificanceValue(double significanceValue) {
        this.significanceValue = significanceValue;
    }

    public int getStateId() {
        return stateId;
    }

    public void setStateId(int stateId) {
        this.stateId = stateId;
    }

    public int getFlowId() {
        return flowId;
    }

    public void setFlowId(int flowId) {
        this.flowId = flowId;
    }

    public int getTimeChangeSignificance() {
        return timeChangeSignificance;
    }

    public void setTimeChangeSignificance(int timeChangeSignificance) {
        this.timeChangeSignificance = timeChangeSignificance;
    }

    public int getMeaning() {
        return meaning;
    }

    public void setMeaning(int meaning) {
        this.meaning = meaning;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LROutcome lrOutcome = (LROutcome) o;
        return Double.compare(lrOutcome.getpValue(), getpValue()) == 0 &&
                Double.compare(lrOutcome.gettValue(), gettValue()) == 0 &&
                Double.compare(lrOutcome.getBeta1(), getBeta1()) == 0 &&
                Double.compare(lrOutcome.getBeta0(), getBeta0()) == 0 &&
                Double.compare(lrOutcome.getSignificanceValue(), getSignificanceValue()) == 0 &&
                getStateId() == lrOutcome.getStateId() &&
                getFlowId() == lrOutcome.getFlowId() &&
                getTimeChangeSignificance() == lrOutcome.getTimeChangeSignificance() &&
                getMeaning() == lrOutcome.getMeaning() &&
                getIndependentVariable().equals(lrOutcome.getIndependentVariable()) &&
                getDateInterval().equals(lrOutcome.getDateInterval()) &&
                getCurrentDate().equals(lrOutcome.getCurrentDate()) &&
                getPeriod().equals(lrOutcome.getPeriod());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIndependentVariable(), getDateInterval(), getCurrentDate(), getPeriod(), getpValue(), gettValue(), getBeta1(), getBeta0(), getSignificanceValue(), getStateId(), getFlowId(), getTimeChangeSignificance(), getMeaning());
    }

    @Override
    public String toString() {
        return getContent();
    }

    private String getContent() {
        return
                stateId +  "," +
                flowId +  "," +
                independentVariable + "," +
                pValue +  "," +
                tValue +  "," +
                beta1 +  "," +
                beta0 +  "," +
                significanceValue +  "," +
                timeChangeSignificance +  "," +
                meaning +  "," +
                dateInterval +  "," +
                currentDate +  "," +
                period;
    }
}
