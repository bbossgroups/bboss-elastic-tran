package org.frameworkset.tran.input.file;
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

import org.frameworkset.util.concurrent.ThreadPoolFactory;

import java.util.concurrent.ExecutorService;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/3/24
 * @author biaoping.yin
 * @version 1.0
 */
public class RemoteFileChannel {
	private int workThreads = 10;
	private int workThreadQueue = 10;
	private long blockedWaitTimeout = -1l;
	private int warnMultsRejects = 5000;
	private ExecutorService executor = null;
	private String threadName = "RemoteFileDownload";
	public void init(){
		executor = ThreadPoolFactory.buildThreadPool(threadName,threadName,
				workThreads,workThreadQueue,
				blockedWaitTimeout
				,warnMultsRejects);
	}
	public void submitNewTask(Runnable runnable){
		executor.submit(runnable);

	}

	public String getThreadName() {
		return threadName;
	}

	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}

	public int getWorkThreads() {
		return workThreads;
	}

	public void setWorkThreads(int workThreads) {
		this.workThreads = workThreads;
	}

	public int getWorkThreadQueue() {
		return workThreadQueue;
	}

	public void setWorkThreadQueue(int workThreadQueue) {
		this.workThreadQueue = workThreadQueue;
	}

	public long getBlockedWaitTimeout() {
		return blockedWaitTimeout;
	}

	public void setBlockedWaitTimeout(long blockedWaitTimeout) {
		this.blockedWaitTimeout = blockedWaitTimeout;
	}

	public int getWarnMultsRejects() {
		return warnMultsRejects;
	}

	public void setWarnMultsRejects(int warnMultsRejects) {
		this.warnMultsRejects = warnMultsRejects;
	}


}
