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
import org.frameworkset.tran.schedule.ExternalScheduler;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>Description: xxl-job 2.x,3.x作业调度抽象类</p>
 * @Date 2019/4/20 22:51
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class AbstractXXLJobHandler extends IJobHandler {
	protected ExternalScheduler externalScheduler;

	private Lock lock = new ReentrantLock();
	@Override
	public abstract void init();
	@Override
	public void execute(){
		lock.lock();
		try {

			String param = XxlJobHelper.getJobParam();
			externalScheduler.execute(  param);

		}
		finally {
			lock.unlock();
		}
	}
	@Override
	public void destroy(){
		if(externalScheduler != null){
			externalScheduler.destroy();
		}
	}


}
