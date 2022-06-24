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

import com.xxl.job.core.handler.IJobHandler;
import org.frameworkset.tran.DataImportException;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/12/3 14:11
 * @author biaoping.yin
 * @version 1.0
 */
public class WrapperXXLJobHandler extends IJobHandler {
	private IJobHandler iJobHandler;
	private boolean inited;
	private Lock lock = new ReentrantLock();
	public WrapperXXLJobHandler(IJobHandler iJobHandler) {
		this.iJobHandler = iJobHandler;
	}

	@Override
	public void init() {
		if(inited )
			return;

		try {
			lock.lock();
			if(inited )
				return;
			try {
				iJobHandler.init();
				inited = true;
			}
			catch (DataImportException e){
				throw e;
			}
			catch (Exception e){
				throw new DataImportException("Init iJobHandler failed:" ,e);
			}
			catch (Throwable e){
				throw new DataImportException("Init iJobHandler failed:" ,e);
			}
			finally {
				if(!inited)
					inited = true;
			}


		}
		finally {
			lock.unlock();
		}
	}
	@Override
	public void execute() throws Exception {
		try {
			init();
//			String param = XxlJobHelper.getJobParam();
			iJobHandler.execute();
		}
		catch (Exception e){
			throw e;
		}
	}
	public boolean isInited() {
		return inited;
	}
	@Override
	public String toString(){
		return iJobHandler.getClass().getName();
	}
}
