package org.frameworkset.tran.util;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/10/12
 * @author biaoping.yin
 * @version 1.0
 */
public class StoppedThread extends Thread{
	private static Logger logger = LoggerFactory.getLogger(StoppedThread.class);
	public StoppedThread() {
	}

	public StoppedThread(Runnable target) {
		super(target);
	}

	public StoppedThread(ThreadGroup group, Runnable target) {
		super(group, target);
	}

	public StoppedThread(String name) {
		super(name);
	}

	public StoppedThread(ThreadGroup group, String name) {
		super(group, name);
	}

	public StoppedThread(Runnable target, String name) {
		super(target, name);
	}

	public StoppedThread(ThreadGroup group, Runnable target, String name) {
		super(group, target, name);
	}

	public StoppedThread(ThreadGroup group, Runnable target, String name, long stackSize) {
		super(group, target, name, stackSize);
	}

	protected volatile boolean stopped;
	private ReentrantLock lock = new ReentrantLock();
	public void stopThread(){
		if(this.stopped)
			return;
		lock.lock();
		try{

			if(stopped)
				return;
			this.stopped = true;
		}
		finally {
			lock.unlock();
		}
		this.interrupt();
	}
}
