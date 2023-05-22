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

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/26 12:41
 * @author biaoping.yin
 * @version 1.0
 */
public class SplitData {
	private Map<String,Object> splitData;
	private Object originData;

	public Map<String, Object> getSplitData() {
		return splitData;
	}

	public void setSplitData(Map<String, Object> splitData) {
		this.splitData = splitData;
	}

	public Object getOriginData() {
		return originData;
	}

	public void setOriginData(Object originData) {
		this.originData = originData;
	}
}
