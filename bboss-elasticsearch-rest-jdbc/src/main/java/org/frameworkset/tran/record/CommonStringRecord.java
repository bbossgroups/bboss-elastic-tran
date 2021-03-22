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

import org.frameworkset.tran.Record;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/19 11:09
 * @author biaoping.yin
 * @version 1.0
 */
public class CommonStringRecord implements Record {
	private Object key;
	private String record;
	private long offset;
	public CommonStringRecord(Object key, String record,long offset){
		this.record = record;
		this.key = key;
		this.offset = offset;
	}
	public CommonStringRecord(String record,long offset){
		this.record = record;
		this.offset = offset;
	}
	@Override
	public Object getValue(String colName) {


		return record;
	}
	public Object getKeys(){
		return null;
	}
	public Object getData(){
		return this;
	}

	@Override
	public Object getMetaValue(String metaName) {
		return null;
	}

	public Object getKey() {
		return key;
	}

	public String getRecord() {
		return record;
	}

	@Override
	public long getOffset() {
		return offset;
	}
}
