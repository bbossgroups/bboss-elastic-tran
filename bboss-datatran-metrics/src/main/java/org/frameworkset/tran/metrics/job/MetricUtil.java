package org.frameworkset.tran.metrics.job;
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
 * @Date 2020/9/21 11:54
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class MetricUtil {
	public static String handleChannelId(Map data){
//渠道编码
		String chanId = (String) data.get("chanId");
		if(chanId == null || "null".equals(chanId) || "".equals(chanId)){
			return "其他";
		}
		return chanId;
	}
}
