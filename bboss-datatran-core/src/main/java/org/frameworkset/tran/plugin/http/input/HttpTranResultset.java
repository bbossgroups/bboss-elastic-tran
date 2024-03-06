package org.frameworkset.tran.plugin.http.input;
/**
 * Copyright 2022 bboss
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

import org.frameworkset.tran.*;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.http.HttpResult;
import org.frameworkset.tran.record.NextAssert;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/30
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpTranResultset extends LastValue implements TranResultSet {
	private HttpResult<Map> httpResult;
	private Map current;
	private Iterator<Map> iterator;
//	private boolean stoped;
	private HttpInputConfig httpInputConfig;

	private QueryAction queryAction;

	public HttpTranResultset(QueryAction queryAction,ImportContext importContext) {
		this.importContext = importContext;
		httpInputConfig = (HttpInputConfig) importContext.getInputConfig();
		this.queryAction = queryAction;


	}
	public  void init(){
		HttpResult<Map> httpResult = queryAction.execute();
		this.httpResult = httpResult;
		List<Map> datas = httpResult.getDatas();
		if(datas != null && datas.size() > 0)
			iterator = datas.iterator();
	}
	@Override
	public TaskContext getRecordTaskContext() {
		return record.getTaskContext();
	}

	@Override
	public Object getValue(int i, String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
	}

	@Override
	public Object getValue(String colName) throws DataImportException {
		return record.getValue(colName);

	}

	@Override
	public Object getValue(String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
	}

	@Override
	public Date getDateTimeValue(String colName) throws DataImportException {
		return record.getDateTimeValue(colName);

	}

	@Override
	public NextAssert next() throws DataImportException {
        NextAssert nextAssert = new NextAssert();
		if(isStop() || iterator == null|| importContext.getInputPlugin().isStopCollectData())
			return nextAssert;
		boolean hasNext = iterator.hasNext();
		if( hasNext){
			current = iterator.next();
			record = new HttpRecord(httpResult,current,getTaskContext());
		}
		else{
			if(queryAction.hasMore()){
				if(isStop() )
					return nextAssert;
				this.httpResult = queryAction.execute();
				List<Map> datas = httpResult.getDatas();
				if(datas != null && datas.size() > 0) {
					iterator = datas.iterator();
					current = iterator.next();
					record = new HttpRecord(httpResult,current,getTaskContext());
					hasNext = true;
				}
			}
		}
        nextAssert.setHasNext(hasNext);
		return nextAssert;
	}

	@Override
	public TranMeta getMetaData() {
		return new DefaultTranMetaData(current.keySet());
	}

	public Object getKeys(){
		return record.getKeys();
	}
	@Override
	public Object getRecord() {
		return current;
	}

	@Override
	public Record getCurrentRecord() {
		return record;
	}

//	@Override
//	public void stop() {
//		stoped = true;
//	}


	@Override
	public Object getMetaValue(String fieldName) {
		return getValue(fieldName);
	}

}
