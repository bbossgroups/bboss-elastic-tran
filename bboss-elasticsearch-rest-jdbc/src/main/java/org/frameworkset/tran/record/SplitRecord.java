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

import org.frameworkset.tran.Record;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/20 11:12
 * @author biaoping.yin
 * @version 1.0
 */
public class SplitRecord extends CommonMapRecord{
	private Record baseRecord;
	public SplitRecord(Record baseRecord, Object key, Map<String, Object> record) {
		super(baseRecord.getTaskContext(), key, record, baseRecord.getOffset());
		this.baseRecord = baseRecord;
	}

	public SplitRecord(Record baseRecord, Map<String, Object> record) {
		super(baseRecord.getTaskContext(), record, baseRecord.getOffset());
	}

	@Override
	public boolean reachEOFClosed(){
		return baseRecord.reachEOFClosed();
	}


	@Override
	public Object getValue(String colName) {

		Object value =  super.getValue(colName);
		if(value == null){
			baseRecord.getValue(colName);
		}
		return value;
	}
	public Object getKeys(){
		return baseRecord.getKeys();
	}
	public Object getData(){
		return baseRecord.getData();
	}

	@Override
	public Object getMetaValue(String metaName) {
		Object value = this.getValue(metaName);
		if(value == null) {
			value = baseRecord.getMetaValue(metaName);
		}
		return value;
	}

	@Override
	public long getOffset() {
		return baseRecord.getOffset();
	}

	@Override
	public boolean removed() {
		return false;
	}

	public Object getKey() {
		return super.getKey();
	}

	public Map<String, Object> getRecord() {
		return super.getRecord();
	}
}
