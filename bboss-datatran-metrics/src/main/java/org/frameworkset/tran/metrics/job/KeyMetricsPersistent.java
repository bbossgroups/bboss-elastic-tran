package org.frameworkset.tran.metrics.job;
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

import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.util.concurrent.ThreadPoolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/6/1 9:44
 * @author biaoping.yin
 * @version 1.0
 */
public class KeyMetricsPersistent {
	private static Logger logger = LoggerFactory.getLogger(KeyMetricsPersistent.class);
	private ExecutorService executor ;
	private int persistentWorkThreads = 10;
	private int persistentWorkThreadQueue = 100;
	private int persistentBlockedWaitTimeout = 0;
	private int persistentWarnMultsRejects = 100;
	private boolean stoped;
	private BaseMetrics persistent;
	private String persistentName;
	public KeyMetricsPersistent(){
		this.persistentName = "KeyMetrics";
	}
	public KeyMetricsPersistent(String persistentName){
		this.persistentName = persistentName;
	}
	public void setPersistent(BaseMetrics persistent) {
		this.persistent = persistent;
	}

    private Object stopLock = new Object();
	public  void stop(){
		synchronized (stopLock) {
			if (stoped)
				return;
			stoped = true;
		}
        logger.info("{}-Persistent begin......",persistentName);
		if(executor != null){

            ThreadPoolFactory.shutdownExecutor(executor);

		}
        logger.info("{}-Persistent completed.",persistentName);
	}
	public void init() {
		executor = ThreadPoolFactory.buildThreadPool(persistentName + "Persistent", persistentName +" Persistent",
				persistentWorkThreads, persistentWorkThreadQueue,
				persistentBlockedWaitTimeout
				, persistentWarnMultsRejects);
	}

	public Future persistent(final Collection<KeyMetric> persistentMetrics){
		return persistent(  persistentMetrics,(FlushCallback)null);
	}

	public Future persistent(final Collection<KeyMetric> persistentMetrics, FlushCallback flushCallback){
        if(stoped){
            throw new MetricsException("metrics persistent has stopped.");
        }
		return executor.submit(new Runnable() {
			@Override
			public void run() {
				persistent.persistent(persistentMetrics);
				if(flushCallback != null) {
					try {
						flushCallback.callback();
					} catch (Exception e) {
						logger.error("flushCallback failed:", e);
					}
				}
			}
		});
	}
}
