package com.kuveytturk.pm.com.kuveytturk.pm.regression;

public class FlowStateBean implements java.io.Serializable{
    private int flowId, stateId;

    public FlowStateBean() {  }

    public FlowStateBean(int flowId, int stateId) {
        this.flowId = flowId;
        this.stateId = stateId;
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
}
