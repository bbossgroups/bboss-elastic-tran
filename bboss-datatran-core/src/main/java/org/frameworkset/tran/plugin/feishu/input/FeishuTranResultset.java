package org.frameworkset.tran.plugin.feishu.input;
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
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.http.HttpResult;
import org.frameworkset.tran.plugin.http.input.HttpInputConfig;
import org.frameworkset.tran.plugin.http.input.HttpRecord;
import org.frameworkset.tran.plugin.http.input.QueryAction;
import org.frameworkset.tran.record.NextAssert;

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
public class FeishuTranResultset extends LastValue implements TranResultSet {
	private FeishuData current;
	private Iterator<FeishuData> iterator;
//	private boolean stoped;
	private FeishuTableInputConfig feishuTableInputConfig;

	private FeishuQueryAction feishuQueryAction;
    private List<FeishuData> feishuTableResult;

    public FeishuTranResultset(FeishuQueryAction queryAction, ImportContext importContext) {
		this.importContext = importContext;
        feishuTableInputConfig = (FeishuTableInputConfig) importContext.getInputConfig();
		this.feishuQueryAction = queryAction;


	}
	public  void init(){
        List<FeishuData> feishuTableResult = feishuQueryAction.execute();
		this.feishuTableResult = feishuTableResult;
		if(feishuTableResult != null && feishuTableResult.size() > 0)
			iterator = feishuTableResult.iterator();
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
            FeishuRecord _record = new FeishuRecord(current.getFields(),getTaskContext(),importContext)
                    .setRecordId(current.getRecordId());
            _record.initMetaDatas();
            record = _record;
            record.setTranMeta(this.getMetaData());
		}
		else{
			if(feishuQueryAction.hasMore()){
				if(isStop() )
					return nextAssert;
				this.feishuTableResult = feishuQueryAction.execute();
				if(feishuTableResult != null && feishuTableResult.size() > 0) {
					iterator = feishuTableResult.iterator();
					current = iterator.next();
					FeishuRecord _record = new FeishuRecord(current.getFields(),getTaskContext(),importContext)
                            .setRecordId(current.getRecordId());
                    _record.initMetaDatas();
                    record = _record;
					hasNext = true;
				}
			}
		}
        nextAssert.setHasNext(hasNext);
		return nextAssert;
	}

	@Override
	public TranMeta getMetaData() {
		return new DefaultTranMetaData(() -> new String[]{"recordId"});
	}

	public Object getKeys(){
		return record.getKeys();
	}
	@Override
	public Object getRecord() {
		return record.getData();
	}


 

	@Override
	public Object getMetaValue(String fieldName) {
        if(fieldName.equals("recordId") || fieldName.equals("meta:recordId")){
            return current.getRecordId();
        }
		return getValue(fieldName);
	}

}
