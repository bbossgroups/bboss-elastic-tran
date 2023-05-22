package org.frameworkset.tran.plugin.kafka.input;
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/12/19
 * @author biaoping.yin
 * @version 1.0
 */
public class KafkaTaskContext extends TaskContext {
	public KafkaTaskContext(ImportContext importContext) {
		super(importContext);
	}

	public KafkaTaskContext() {
		super();
	}
	public static interface ReInitAction {
		public void afterCall(TaskContext taskContext);
		public void preCall(TaskContext taskContext);
	}

	public synchronized void reInitContext(ReInitAction reInitAction){
		// 当有记录到达，才执行
		if(this.getJobTaskMetrics().getTotalRecords() > 0) {
			TaskContext taskContextCopy = copy();
			reInitAction.afterCall(taskContextCopy);
			super.initContext();
			reInitAction.preCall(this);
		}
	}

	@Override
	protected TaskContext createTaskContext(){
		return new KafkaTaskContext();
	}


}
