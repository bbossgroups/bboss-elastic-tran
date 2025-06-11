package org.frameworkset.tran.schedule.quartz;
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

import org.frameworkset.tran.jobflow.schedule.ExternalJobFlowScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 工作流quartz调度任务基础类
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class AbstractQuartzJobFlowHandler {
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	protected ExternalJobFlowScheduler externalJobFlowScheduler;
	private Lock lock = new ReentrantLock();

    /**
     * 通过init方法初始化一个作业工作流构建器
     */
	public abstract void init();
	public void execute(){
		lock.lock();
		try {

            externalJobFlowScheduler.execute(null);

		}
		finally {
			lock.unlock();
		}
	}

	public void destroy(){
		if(externalJobFlowScheduler != null){
            externalJobFlowScheduler.destroy();
		}
	}
}
