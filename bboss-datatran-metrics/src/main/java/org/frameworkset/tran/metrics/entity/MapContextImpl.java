package org.frameworkset.tran.metrics.entity;
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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/8/27
 * @author biaoping.yin
 * @version 1.0
 */
public class MapContextImpl implements MapContext{
	private Map<String,Object> contextParams ;
	public MapContext addContextParam(String param,Object value){
		if(contextParams == null){
			contextParams = new LinkedHashMap<>();

		}
		contextParams.put(param,value);
		return this;
	}

	public <T> T getContextParam(String param,Class<T> type){
		if(contextParams != null)
			return (T)contextParams.get(param);
		return null;
	}

	public <T> T getContextParam(String param,Class<T> type,T defaultValue){
		T value = null;
		if(contextParams != null)
			value = (T)contextParams.get(param);
		if(value != null) {
			return value;
		}
		else {
			return defaultValue;
		}
	}
}
