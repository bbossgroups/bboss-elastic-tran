package org.frameworkset.tran.hbase;
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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.util.TranUtil;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/8/3 12:27
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseResultSet extends LastValue implements TranResultSet {
	private ResultScanner resultScanner;
	private Record record;
	private boolean incrementByTimeRange;

	private Map<String,byte[][]> familys;
	public HBaseResultSet(ImportContext importContext, ResultScanner resultScanner) {
		this.importContext = importContext;
		this.resultScanner = resultScanner;
		familys = new HashMap<String, byte[][]>();
		incrementByTimeRange = ((HBaseContextImpl)importContext).isIncrementByTimeRange();
	}


	@Override
	public Object getValue(int i, String colName, int sqlType) throws ESDataImportException {
		return getValue(  colName);
	}

	@Override
	public Object getValue(String colName) throws ESDataImportException {
		//todo fixed
		Object value = record.getValue(colName);

		return value;

	}

	@Override
	public Object getValue(String colName, int sqlType) throws ESDataImportException {
		return getValue(  colName);
	}

	@Override
	public Date getDateTimeValue(String colName) throws ESDataImportException {
		Object value = getValue(  colName);
		if(value == null)
			return null;
		Long time = Bytes.toLong((byte[])value);
		return TranUtil.getDateTimeValue(colName,time,importContext);

	}



	@Override
	public Boolean next() throws ESDataImportException {
		try {
			Result record = resultScanner.next();
			if( record != null){
				this.record = new HBaseRecord(familys,record);
				return true;
			}
		} catch (IOException e) {
			throw new ESDataImportException("Get next hbase result failed:",e);
		}

		return false;
	}

	@Override
	public TranMeta getMetaData() {
		return new DefaultTranMetaData(null);
	}

	@Override
	public Object getRecord() {
		return record;
	}

	@Override
	public void stop() {

	}

	@Override
	public Object getMetaValue(String fieldName) {
		return record.getMetaValue(fieldName);
	}
	public Object getLastValue(TranResultSet tranResultSet,ImportContext importContext,String colName) throws ESDataImportException{
		try {
			if (importContext.getLastValueType() == null || importContext.getLastValueType().intValue() == ImportIncreamentConfig.NUMBER_TYPE) {
				Object value = tranResultSet.getValue(importContext.getLastValueColumnName());
				Long l = Bytes.toLong((byte[])value);
				return l;
			}
			else if(incrementByTimeRange){
				return tranResultSet.getMetaValue("timestamp");
			}
			else if (importContext.getLastValueType().intValue() == ImportIncreamentConfig.TIMESTAMP_TYPE) {
				return tranResultSet.getDateTimeValue(importContext.getLastValueColumnName());
			}
		}
		catch (ESDataImportException e){
			throw (e);
		}
		catch (Exception e){
			throw new ESDataImportException(e);
		}
		throw new ESDataImportException("Unsupport last value type:"+importContext.getLastValueType().intValue());
	}
}
