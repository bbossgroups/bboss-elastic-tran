package org.frameworkset.tran;
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

import org.frameworkset.tran.context.ImportContext;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:46
 * @author biaoping.yin
 * @version 1.0
 */
public class TranErrorWrapper {
	protected ImportContext importContext;

	/**
	 * see https://www.cnblogs.com/dolphin0520/p/3920373.html
	 */
	protected volatile Exception error;
	private Lock lock = new ReentrantLock();
	public TranErrorWrapper(ImportContext importContext){
		this.importContext = importContext;
	}
	public Exception throwError() throws Exception{
		return error;
	}
	public void setError(Exception error) {
		if(this.error == null) {//only set the first exception
			lock.lock();
			try {

				if (this.error == null) {//only set the first exception
					this.error = error;
//					dataTranPlugin.setErrorWrapper(this);
				}
			} finally {
				lock.unlock();
			}
		}
	}

	/**
	 * 判断异常情况下是否继续允许作业，返回true，继续执行，否则中断执行
	 * @return
	 */
	public boolean assertCondition(){

		return assertCondition(error);
	}

	/**
	 * 判断执行条件是否成立，成立返回true，否则返回false
	 * @return
	 */
	public boolean assertCondition(Exception e){
		if(error == null && e != null){
			this.setError(e);
		}
        stopFromError = e != null && !importContext.isContinueOnError();
        if(stopFromError)
            return false;
		return assertCondition(error,importContext);
	}

    public boolean isStopFromError() {
        return stopFromError;
    }

    private boolean stopFromError ;
	/**
	 * 判断执行条件是否成立，成立返回true，否则返回false
	 * @return
	 */
	private static boolean assertCondition(Throwable e,ImportContext importContext){

//		if(e != null && !importContext.isContinueOnError() ) {
//
//			return false;
//		}
		if(importContext.isCurrentStoped()){
			return false;
		}
		return true;
	}

}
