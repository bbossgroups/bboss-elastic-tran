package org.frameworkset.tran;
/**
 * Copyright 2008 biaoping.yin
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;

import static org.frameworkset.tran.util.TranConstant.*;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2020/2/1 23:46
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class LastValue implements TranResultSet{
	protected ImportContext importContext;
	protected BaseDataTran baseDataTran;
	protected Record record;
    private volatile int status = STATUS_DEFAULT;
	public BaseDataTran getBaseDataTran(){
		return baseDataTran;
	}
    private Object stopLock = new Object();
    public boolean isStop(){
        synchronized (stopLock) {
            return status == STATUS_STOP_NORMAL || status == STATUS_STOP_EXCEPTION;
        }
    }
    @Override
    public TaskContext getRecordTaskContext() {
        return record.getTaskContext();
    }

    @Override
    public Record getCurrentRecord() {
        return record;
    }

    public boolean isStop(boolean forceStop){
        if(forceStop) {
            return isStop();
        }
        else{
            return isStopFromException();
        }
    }

    public boolean isStopFromException(){
        synchronized (stopLock) {
            return status == STATUS_STOP_EXCEPTION;
        }
    }
    @Override
    public void stop(boolean exception){
        if(status != STATUS_DEFAULT){
            return;
        }
        synchronized (stopLock) {
            if(status != STATUS_DEFAULT){
                return;
            }
            if(!exception) {
                status = STATUS_STOP_NORMAL;
            }
            else{
                status = STATUS_STOP_EXCEPTION;
            }
        }
    }
    public Map<String, Object> getMetaDatas(){
        return record.getMetaDatas();
    }
 
    public int getAction(){
        return record.getAction();
    }
	public TaskContext getTaskContext(){
		return this.baseDataTran.getTaskContext();
	}
	public void setBaseDataTran(BaseDataTran baseDataTran){
		this.baseDataTran = baseDataTran;
	}
	public void setImportContext(ImportContext importContext) {
		this.importContext = importContext;
	}

    @Override
    public Object getValue(String colName) throws DataImportException {
        //todo fixed
        Object value = record.getValue(colName);

        return value;

    }
	public Object getLastValue(String colName) throws DataImportException {
		try {
			if (importContext.getLastValueType() == null || importContext.getLastValueType().intValue() == ImportIncreamentConfig.NUMBER_TYPE)
				return getValue(importContext.getLastValueColumnName());
			else if (importContext.getLastValueType().intValue() == ImportIncreamentConfig.TIMESTAMP_TYPE) {
				if(importContext.getLastValueDateformat() == null || importContext.getLastValueDateformat().equals("")) {
					return getDateTimeValue(importContext.getLastValueColumnName());
				}
				else{
					return getDateTimeValue(importContext.getLastValueColumnName(),importContext.getLastValueDateformat());
				}
			}
            else if (importContext.getLastValueType().intValue() == ImportIncreamentConfig.LOCALDATETIME_TYPE) {
                return getLocalDateTimeValue(importContext.getLastValueColumnName());
            }
            else if (importContext.getLastValueType().intValue() == ImportIncreamentConfig.STRING_TYPE){
                if(importContext.getLastValueColumnName() != null) {
                    return getValue(importContext.getLastValueColumnName());
                }
                else{
                    return null;
                }
            }
		}
		catch (DataImportException e){
			throw (e);
		}
		catch (Exception e){
			throw new DataImportException(e);
		}
		throw new DataImportException("Unsupport last value type:"+importContext.getLastValueType().intValue());
	}
	public Object getLastOffsetValue() throws DataImportException {
		Record record = this.getCurrentRecord();
		return record.getOffset();
	}

    public Object getLastValue(){
        if(!importContext.useFilePointer()) {
            if (importContext.getLastValueColumnName() == null) {
                return -1;
            }
            return getLastValue(importContext.getLastValueColumnName());
        }
        else{
            return getLastOffsetValue();
        }
    }
 

	@Override
	public Date getDateTimeValue(String colName) throws DataImportException {
		return record.getDateTimeValue(colName);

	}

	@Override
	public Date getDateTimeValue(String colName,String format) throws DataImportException {
		return record.getDateTimeValue(colName,format);

	}

    @Override
    public LocalDateTime getLocalDateTimeValue(String colName) throws DataImportException {
        return record.getLocalDateTimeValue(colName);

    }
    public boolean isRecordDirectIgnore(){
        return getAction() == Record.RECORD_DIRECT_IGNORE;
    }
}
