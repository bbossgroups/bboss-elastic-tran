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
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.plugin.hbase.input.HBaseInputConfig;
import org.frameworkset.tran.record.NextAssert;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;

import java.io.IOException;
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
//	protected boolean stoped;
	private boolean incrementByTimeRange;

	private Map<String,byte[][]> familys;
	public HBaseResultSet(ImportContext importContext, ResultScanner resultScanner) {
		this.importContext = importContext;
		this.resultScanner = resultScanner;
		familys = new HashMap<String, byte[][]>();
		HBaseInputConfig hBaseInputConfig = (HBaseInputConfig) importContext.getInputConfig();
		incrementByTimeRange = hBaseInputConfig.isIncrementByTimeRange();
	}



	@Override
	public Object getValue(int i, String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
	}



	@Override
	public Object getValue(String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
	}





	@Override
	public NextAssert next() throws DataImportException {
        NextAssert nextAssert = new NextAssert();
		try {
			if(isStop() || importContext.getInputPlugin().isStopCollectData())
				return nextAssert;
			Result record = resultScanner.next();
			if( record != null){
				this.record = new HBaseRecord(getTaskContext(),importContext,familys,record);
                this.record.setTranMeta(this.getMetaData());
                nextAssert.setHasNext(true);
				return nextAssert;
			}
		} catch (IOException e) {
			throw ImportExceptionUtil.buildDataImportException(importContext,"Get next hbase result failed:",e);
		}

		return nextAssert;
	}

	@Override
	public TranMeta getMetaData() {
		return new DefaultTranMetaData(null);
	}
	public Object getKeys(){
		return  record.getKeys();
	}
	@Override
	public Object getRecord() {
		return record.getData();
	}

//	@Override
//	public void stop() {
//		stoped = true;
//	}

	@Override
	public Object getMetaValue(String fieldName) {
		return record.getMetaValue(fieldName);
	}
	public Object getLastValue(TranResultSet tranResultSet,ImportContext importContext,String colName) throws DataImportException {
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
		catch (DataImportException e){
			throw (e);
		}
		catch (Exception e){
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}
		throw ImportExceptionUtil.buildDataImportException(importContext,"Unsupport last value type:"+importContext.getLastValueType().intValue());
	}
	
}
