package org.frameworkset.tran.record;
/**
 * Copyright 2020 bboss
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.TranUtil;

import java.util.Date;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/8/4 16:27
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseRecord implements Record {
	protected transient TaskContext taskContext;
    protected boolean readEOFRecord;
    protected boolean removed;
    protected boolean reachEOFClosed;
    protected Map<String,Object> metaDatas;
	public BaseRecord(TaskContext taskContext){
		this.taskContext = taskContext;
	}

    public BaseRecord(TaskContext taskContext,boolean removed,
                     boolean reachEOFClosed,boolean readEOFRecord){
        this.taskContext = taskContext;
        this.removed = removed;
        this.reachEOFClosed = reachEOFClosed;
        this.readEOFRecord = readEOFRecord;
    }

	@Override
	public TaskContext getTaskContext() {
		return taskContext;
	}

	public void setTaskContext(TaskContext taskContext) {
		this.taskContext = taskContext;
	}
	@Override
	public Date getDateTimeValue(String colName) throws DataImportException {
		Object value = getValue(  colName);
		if(value == null)
			return null;
		return TranUtil.getDateTimeValue(colName,value,taskContext.getImportContext());

	}

	@Override
	public Date getDateTimeValue(String colName,String dateformat) throws DataImportException {
		Object value = getValue(  colName);
		if(value == null)
			return null;
		return TranUtil.getDateTimeValue(colName,value,taskContext.getImportContext(),dateformat);

	}
    @Override
    public boolean reachEOFRecord(){
        return readEOFRecord;
    }

    @Override
    public boolean reachEOFClosed(){
        return reachEOFClosed;
    }
    @Override
    public boolean removed() {
        return removed;
    }

    public Map<String, Object> getMetaDatas() {
        return metaDatas;
    }

    public void setMetaDatas(Map<String, Object> metaDatas) {
        this.metaDatas = metaDatas;
    }

    protected Object _getMetaValue(String metaName){
        if(metaDatas.containsKey(metaName))
            return metaDatas.get(metaName);
        else{
            throw new DataImportException(new StringBuilder().append("Get Meta Value failed: ").append( metaName ).append( " is not a meta field").append( SimpleStringUtil.object2json(metaDatas.keySet())).append(".").toString());
        }
    }
    @Override
    public Object getMetaValue(String metaName) {
        if(this.metaDatas != null){
            return _getMetaValue(metaName);
        }
        return this.getValue(metaName);
    }
    /**
     * 获取binlog采集的修改前记录信息
     * @return
     */
    @Override
    public Map<String, Object> getUpdateFromDatas(){
        return null;
    }

    @Override
    public int getAction(){
        return Record.RECORD_INSERT;
    }
}
