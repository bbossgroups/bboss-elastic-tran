package org.frameworkset.tran.kafka.output;
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseTaskCommand;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/26 17:07
 * @author biaoping.yin
 * @version 1.0
 */
public class KafkaCommand  extends BaseTaskCommand {

	public KafkaCommand(ImportCount importCount, ImportContext importContext, ImportContext targetImportContext,
						long dataSize, int taskNo, String jobNo, Object lastValue, TaskContext context) {
		super(importCount,importContext,  targetImportContext,  dataSize,  taskNo,  jobNo,lastValue);
		this.taskContext = context;
	}
	private Object key;
	private Object datas;

	@Override
	public Object getDatas() {
		return datas;
	}

	@Override
	public void setDatas(Object datas) {
		this.datas = datas;
	}

	@Override
	public Object execute() {
		 ((KafkaOutputContext)targetImportContext).getKafkaSend().send(this,taskContext,key,datas,importContext.getExportResultHandler());
		 return null;
	}

	@Override
	public int getTryCount() {
		return 0;
	}

	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}
}
