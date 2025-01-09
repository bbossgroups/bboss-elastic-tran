package org.frameworkset.tran.plugin.es.input;
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

import org.frameworkset.elasticsearch.client.ResultUtil;
import org.frameworkset.tran.BaseExportResultHandler;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.task.TaskCommand;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 10:20
 * @author biaoping.yin
 * @version 1.0
 */
public class ESExportResultHandler extends BaseExportResultHandler<String> {
	public ESExportResultHandler(ExportResultHandler exportResultHandler, OutputConfig outputConfig){
		super(exportResultHandler,   outputConfig);
	}


	/**
	 * 处理导入数据结果，如果失败则可以通过重试失败数据
	 * @param taskCommand
	 * @param result
	 *
	 */
	public void handleResult(TaskCommand<String> taskCommand, String result){


		if(!ResultUtil.bulkResponseError(result)){
			success(  taskCommand,   result);
		}
		else{
			error(  taskCommand,   result);
		}

	}




}
