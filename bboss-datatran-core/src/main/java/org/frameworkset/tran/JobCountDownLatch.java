package org.frameworkset.tran;
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/11/10
 * @author biaoping.yin
 * @version 1.0
 */
public class JobCountDownLatch {
	private CountDownLatch countDownLatch;
	private Throwable exception;
	public JobCountDownLatch(int count){
		countDownLatch = new CountDownLatch(count);
	}

	public void attachException(Throwable exception){
		this.exception = exception;
	}

	public Throwable getException() {
		return exception;
	}

	public void countDown(){
		countDownLatch.countDown();
	}
	public long getCount(){
		return countDownLatch.getCount();
	}

	public void await() throws InterruptedException {
		countDownLatch.await();
	}

	public boolean await(long timeout, TimeUnit unit)
			throws InterruptedException {
		return countDownLatch.await( timeout,  unit);
	}

}
