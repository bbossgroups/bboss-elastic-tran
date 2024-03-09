package org.frameworkset.tran.record;
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/19 11:09
 * @author biaoping.yin
 * @version 1.0
 */
public class CommonMapRecord extends BaseRecord {
	private Object key;
	private Map<String,Object> record;
	private long offset;
	public CommonMapRecord(TaskContext taskContext, ImportContext importContext, Object key, Map<String,Object> record, long offset){
		super(taskContext,  importContext);
		this.record = record;
		this.key = key;
		this.offset = offset;
	}

	public CommonMapRecord(TaskContext taskContext,ImportContext importContext, Map<String,Object> record,long offset){
		super(taskContext,  importContext);
		this.record = record;
		this.offset = offset;
	}
    public CommonMapRecord(TaskContext taskContext,ImportContext importContext, Map<String,Object> record,boolean removed,long offset,

                           boolean reachEOFClosed,boolean readEOFRecord){
        super(taskContext,  importContext,removed,reachEOFClosed,  readEOFRecord);
        this.record = record;
        this.offset = offset;
    }

    public CommonMapRecord(TaskContext taskContext,ImportContext importContext, Map<String,Object> record,boolean removed,long offset,

                          boolean readEOFRecord){
        super(taskContext,  importContext,removed, readEOFRecord);
        this.record = record;
        this.offset = offset;
    }

    public CommonMapRecord(TaskContext taskContext,ImportContext importContext, Map<String,Object> record,boolean removed,boolean reachEOFClosed,boolean readEOFRecord){
        super(taskContext,  importContext,removed,reachEOFClosed,  readEOFRecord);
        this.record = record;
    }

    public CommonMapRecord(TaskContext taskContext,ImportContext importContext, Map<String,Object> record,boolean removed,boolean readEOFRecord){
        super(taskContext,  importContext,removed, readEOFRecord);
        this.record = record;
    }

	public CommonMapRecord(TaskContext taskContext,ImportContext importContext, Map<String,Object> record){
		super(taskContext,  importContext);
		this.record = record;
	}



	@Override
	public Object getValue(int i, String colName, int sqlType) throws DataImportException {
		return getValue(colName);
	}

	@Override
	public Object getValue(String colName, int sqlType) throws DataImportException {
		return getValue(colName);
	}

//	@Override
//	public Date getDateTimeValue(String colName) throws ESDataImportException {
//		Object value = getValue(  colName);
//		if(value == null)
//			return null;
//		return TranUtil.getDateTimeValue(colName,value,taskContext.getImportContext());
//
//	}

	@Override
	public Object getValue(String colName) {

		return record.get(colName);
	}
	public Object getKeys(){
        if(record == null)
            return null;
		return record.keySet();
	}
	public Object getData(){
		return this.record;
	}



	@Override
	public long getOffset() {
		return offset;
	}



	public Object getKey() {
		return key;
	}


}
