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
	public CommonMapRecord(TaskContext taskContext,Object key, Map<String,Object> record, long offset){
		super(taskContext);
		this.record = record;
		this.key = key;
		this.offset = offset;
	}

	public CommonMapRecord(TaskContext taskContext, Map<String,Object> record,long offset){
		super(taskContext);
		this.record = record;
		this.offset = offset;
	}
	@Override
	public boolean reachEOFClosed(){
		return false;
	}


	@Override
	public Object getValue(String colName) {

		return record.get(colName);
	}
	public Object getKeys(){
		return record.keySet();
	}
	public Object getData(){
		return this;
	}

	@Override
	public Object getMetaValue(String metaName) {
		return null;
	}

	@Override
	public long getOffset() {
		return offset;
	}

	@Override
	public boolean removed() {
		return false;
	}

	public Object getKey() {
		return key;
	}

	public Map<String, Object> getRecord() {
		return record;
	}
}
