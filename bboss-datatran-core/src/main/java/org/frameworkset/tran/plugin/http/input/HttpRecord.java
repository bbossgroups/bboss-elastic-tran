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

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.http.HttpResult;
import org.frameworkset.tran.record.CommonMapRecord;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/30
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpRecord  extends CommonMapRecord {
	private HttpResult<Map> httpResult;
	public HttpRecord(HttpResult<Map> httpResult, Map record, TaskContext taskContext, ImportContext importContext) {
		super(taskContext,  importContext,record);
		this.httpResult = httpResult;
	}

	public ClassicHttpResponse getResponse(){
		return httpResult.getResponse();
	}
	public HttpResult<Map> getHttpResult() {
		return httpResult;
	}
}
