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

import org.frameworkset.elasticsearch.entity.KeyMap;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.*;

/**
 * <p>Description: 支持记录切割功能</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/20 19:31
 * @author biaoping.yin
 * @version 1.0
 */
public class SplitTranResultSet  implements TranResultSet {
	private TranResultSet tranResultSet;
	private List<KeyMap> splitRecords;
	private SplitHandler splitHandler;
	private ImportContext importContext;
	private int splitSize;
	private int splitPos;
	private Record record;

	private Record baseRecord;
	public SplitTranResultSet(ImportContext importContext, TranResultSet tranResultSet){
		this.tranResultSet = tranResultSet;

		this.importContext = importContext;
		this.splitHandler = this.importContext.getSplitHandler();
	}
	@Override
	public Record getCurrentRecord() {
		if(record != null) {
			return record;
		}
		else{
			return tranResultSet.getCurrentRecord();
		}
	}

	@Override
	public Object getValue(int i, String colName, int sqlType) throws ESDataImportException {
		if(record != null)
			return record.getValue(i,colName,sqlType);
		else {
			return tranResultSet.getValue(i,colName,sqlType );
		}

	}

	@Override
	public Object getValue(String colName) throws ESDataImportException {
		if(record != null)
			return record.getValue(colName);
		else {
			return tranResultSet.getValue(colName);
		}
	}

	@Override
	public Object getLastValue(String colName) throws ESDataImportException {
		return tranResultSet.getLastValue(colName);
	}

	@Override
	public Object getLastOffsetValue() throws ESDataImportException {
		return tranResultSet.getLastOffsetValue();
	}

	@Override
	public Object getValue(String colName, int sqlType) throws ESDataImportException {
		if(record != null){
			return record.getValue(colName,sqlType);
		}
		else {
			return tranResultSet.getValue(colName, sqlType);
		}
	}

	@Override
	public BaseDataTran getBaseDataTran() {
		return tranResultSet.getBaseDataTran();
	}

	@Override
	public TaskContext getTaskContext() {
		return tranResultSet.getTaskContext();
	}

	@Override
	public void setBaseDataTran(BaseDataTran baseDataTran) {
		tranResultSet.setBaseDataTran(baseDataTran);
	}

	@Override
	public Date getDateTimeValue(String colName) throws ESDataImportException {
		if(record != null){
			return record.getDateTimeValue(colName);
		}
		else {
			return tranResultSet.getDateTimeValue(colName);
		}
	}

	@Override
	public TaskContext getRecordTaskContext() {
		return tranResultSet.getRecordTaskContext();
	}
	private boolean readEnd(){
		if(splitRecords != null && splitRecords.size() > 0){
			if(splitPos == splitSize)
			{
				return true;
			}
			else{
				return false;
			}
		}
		else
			return true;
	}

	@Override
	public Boolean next() throws ESDataImportException {
		if(!readEnd() ){
			KeyMap<String,Object> keyMap = splitRecords.get(splitPos);
			if(keyMap.getKey() != null)
				record = new SplitRecord(baseRecord,keyMap.getKey(),keyMap);
			else
				record = new SplitRecord(baseRecord,keyMap);
			splitPos ++;
			return true;
		}
		else {
			record = null;
			splitPos = 0;
			splitSize = 0;
			baseRecord = null;
			splitRecords = null;
			Boolean hasNext = tranResultSet.next();
			if(hasNext == null){
				return hasNext;
			}
			if(hasNext) {
				Record baseRecord = tranResultSet.getCurrentRecord();
				if(baseRecord.removed()){//标记为removed状态的记录不需要切割
					return hasNext;
				}
				List<KeyMap> splitRecords_ = splitHandler.splitField(getTaskContext(), baseRecord,
						baseRecord.getValue(getTaskContext().getImportContext().getSplitFieldName()));
				if (splitRecords_ == null || splitRecords_.size() == 0) {
					record = null;
					splitPos = 0;
					splitRecords = null;
					return hasNext;
				}
				else {
					this.baseRecord = baseRecord;
					splitRecords = splitRecords_;
					splitSize = splitRecords.size();
					KeyMap<String, Object> keyMap = splitRecords.get(0);
					if (keyMap.getKey() != null)
						record = new SplitRecord(baseRecord, keyMap.getKey(), keyMap);
					else
						record = new SplitRecord(baseRecord, keyMap);
					splitPos ++;
				}

			}
			return hasNext;

		}
	}

	@Override
	public TranMeta getMetaData() {
		return tranResultSet.getMetaData();
	}

	@Override
	public Object getRecord() {
		if(record != null){
			Object data = tranResultSet.getRecord();
			if(data == null)
				return record.getData();
			if(data instanceof Map){
				Map newData = new LinkedHashMap();
				newData.putAll((Map)data);
				newData.putAll((Map)(record.getData()));
				return newData;
			}
			else{
				SplitData splitData = new SplitData();
				splitData.setOriginData(data);
				splitData.setSplitData((Map)record.getData());
				return splitData;
			}
		}
		else {
			return tranResultSet.getRecord();
		}
	}

	@Override
	public void stop() {
		tranResultSet.stop();
	}

	@Override
	public void stopTranOnly() {
		tranResultSet.stopTranOnly();
	}

	@Override
	public Object getKeys() {
		if(record == null){
			return tranResultSet.getKeys();
		}
		Set<String> cKeys = (Set<String>) record.getKeys();
		String[] columns = cKeys.toArray(new String[cKeys.size()]);
		SplitKeys splitKeys = new SplitKeys();
		splitKeys.setBaseKeys(tranResultSet.getKeys());
		splitKeys.setSplitKeys(columns);
		return splitKeys;
	}

	@Override
	public Object getMetaValue(String fieldName) {
		return tranResultSet.getMetaValue(fieldName);
	}

	@Override
	public boolean removed() {
		return tranResultSet.removed();
	}

	@Override
	public boolean reachEOFClosed() {
		return tranResultSet.reachEOFClosed();
	}



}
