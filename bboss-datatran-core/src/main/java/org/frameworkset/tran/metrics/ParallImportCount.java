package org.frameworkset.tran.metrics;
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

import org.frameworkset.tran.BaseDataTran;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/10/15 19:20
 * @author biaoping.yin
 * @version 1.0
 */
public class ParallImportCount extends ImportCount{
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private Lock readLock = lock.readLock();
	private Lock writeLock = lock.writeLock();

	public ParallImportCount(BaseDataTran baseDataTran){
		super(  baseDataTran);
	}
	public long getTotalCount() {
		readLock.lock();
		try {

			return totalCount;
		}
		finally {
			readLock.unlock();
		}
	}

	public long getFailedCount() {
		readLock.lock();
		try {

			return failedCount;
		}
		finally {
			readLock.unlock();
		}
	}


	public long getIgnoreTotalCount() {
		readLock.lock();
		try {

			return ignoreTotalCount;
		}
		finally {
			readLock.unlock();
		}

	}
	public long[] increamentFailedCount(long failedCount) {
		writeLock.lock();
		try {

			this.failedCount = failedCount+this.failedCount;
			this.totalCount = totalCount + failedCount;
			return new long[]{this.failedCount,this.totalCount};
		}finally {
			writeLock.unlock();
		}
	}
	public long increamentIgnoreTotalCount() {
		writeLock.lock();
		try {

			this.ignoreTotalCount ++;
			this.totalCount ++;
			return ignoreTotalCount;
		}finally {
			writeLock.unlock();
		}
	}


	public long getSuccessCount() {
		readLock.lock();
		try {

			return successCount;
		}
		finally {
			readLock.unlock();
		}

	}
	public long[] increamentSuccessCount(long successCount) {
		writeLock.lock();
		try {

			this.successCount = this.successCount+successCount;
			this.totalCount = totalCount + successCount;
			return new long[]{this.successCount,this.totalCount};
		}finally {
			writeLock.unlock();
		}
	}



}
