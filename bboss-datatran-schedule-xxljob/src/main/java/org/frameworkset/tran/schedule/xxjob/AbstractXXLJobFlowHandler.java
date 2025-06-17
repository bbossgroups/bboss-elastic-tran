package org.frameworkset.tran.schedule.xxjob;
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

import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.IJobHandler;
import org.frameworkset.tran.jobflow.schedule.ExternalJobFlowScheduler;
import org.frameworkset.tran.jobflow.schedule.JobFlowBuilderFunction;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>Description: xxl-job 2.x,3.x作业工作流调度抽象类</p>  
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class AbstractXXLJobFlowHandler extends IJobHandler {
	protected ExternalJobFlowScheduler externalJobFlowScheduler;

	private Lock lock = new ReentrantLock();
	@Override
	public void init(){
        externalJobFlowScheduler = new ExternalJobFlowScheduler();
        externalJobFlowScheduler.setJobFlowBuilderFunction(buildJobFlowBuilderFunction());
    }
    protected abstract JobFlowBuilderFunction buildJobFlowBuilderFunction();
    
	@Override
	public void execute(){
		lock.lock();
		try {

			String param = XxlJobHelper.getJobParam();
            externalJobFlowScheduler.execute(  param);

		}
		finally {
			lock.unlock();
		}
	}
	@Override
	public void destroy(){
		if(externalJobFlowScheduler != null){
            externalJobFlowScheduler.destroy();
		}
	}


}
