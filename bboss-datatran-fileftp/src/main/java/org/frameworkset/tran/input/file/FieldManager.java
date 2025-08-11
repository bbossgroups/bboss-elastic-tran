package org.frameworkset.tran.input.file;
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

import org.frameworkset.tran.record.FieldMappingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/8/27 11:23
 * @author biaoping.yin
 * @version 1.0
 */
public class FieldManager<T extends FieldManager<T>> extends FieldMappingManager<T> {
	private static final Logger logger = LoggerFactory.getLogger(FieldManager.class);
	public FieldManager(){
		super();
	}
	/**
	 * 需要添加的字段
	 */
	protected Map<String,Object> addFields;
	/**
	 * 需要忽略的字段
	 */
	protected Map<String,Object> ignoreFields;
	public T addField(String name,Object value){
		if(addFields == null)
			addFields = new LinkedHashMap<>();
		addFields.put(name,value);
		if(this instanceof FileConfig)
			return (T)this;
		else
			return null;
	}
	public T addFields(Map<String,Object> values){
		if(addFields == null)
			addFields = new LinkedHashMap<>();
		addFields.putAll(values);
		if(this instanceof FileConfig)
			return (T)this;
		else
			return null;
	}

	public Map<String, Object> getAddFields() {
		return addFields;
	}

	public Map<String, Object> getIgnoreFields() {
		return ignoreFields;
	}

	public T ignoreField(String name){
		if(ignoreFields == null)
			ignoreFields = new LinkedHashMap<>();
		ignoreFields.put(name,1);
		if(this instanceof FileConfig)
			return (T)this;
		else
			return null;
	}

	public T ignoreFields(Map<String,Object> ignoreFields){
		if(ignoreFields == null)
			ignoreFields = new LinkedHashMap<>();
		ignoreFields.putAll(ignoreFields);
		if(this instanceof FileConfig)
			return (T)this;
		else
			return null;
	}

}
