package org.frameworkset.tran.status;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/7/26 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public class StatusFlushThread extends Thread{
	private StatusManager statusManager;
	private long flushInterval = 10000;
	private static Logger logger = LoggerFactory.getLogger(StatusFlushThread.class);
	public StatusFlushThread(StatusManager statusManager,long flushInterval){
		super("StatusFlushThread");
		this.setDaemon(true);
		this.statusManager = statusManager;
		this.flushInterval = flushInterval;
	}

	public void run(){
		while (true){
			try {

				sleep(flushInterval);
				try {
					statusManager.flushStatus();
				}
				catch (Exception e){
					logger.warn("StatusManager flush Status failed:",e);
				}
				if(statusManager.isStoped())
					break;
			} catch (InterruptedException e) {//线程中断后，需强制刷新status
				try {
					statusManager.flushStatus();
				}
				catch (Exception e1){
					logger.warn("StatusManager flush Status failed:",e1);
				}
				break;
			}


		}
	}
}
