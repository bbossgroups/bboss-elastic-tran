package org.frameworkset.tran.cdc;

/**
 *
 */
public abstract class CDCData {
    protected Object data;
    protected Object oldValues;
    protected int action;

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Object getOldValues() {
        return oldValues;
    }

    public void setOldValues(Object oldValues) {
        this.oldValues = oldValues;
    }

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
    }
}
