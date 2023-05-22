package org.frameworkset.tran.metrics.job;
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

import org.frameworkset.tran.metrics.entity.TimeMetric;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/8/23
 * @author biaoping.yin
 * @version 1.0
 */
public class FlushOlder  extends Thread{
	private KeyTimeMetrics keyTimeMetrics;
	public FlushOlder(){
		super("KeyTimeMetrics-FlushOlder");
	}

	public void setKeyTimeMetrics(KeyTimeMetrics keyTimeMetrics) {
		this.keyTimeMetrics = keyTimeMetrics;
	}

	private boolean stop;
	private BlockingQueue<Map<String, Map<String, TimeMetric>>> queue = new java.util.concurrent.ArrayBlockingQueue<Map<String,Map<String, TimeMetric>>>(5);
	public void add(Map<String,Map<String, TimeMetric>> timeMetrics){

		try {
			queue.put(timeMetrics);
		} catch (InterruptedException e) {

		}
	}
	public void stopFlush(){
		if(stop)
			return;
		synchronized (this){
			if(stop)
				return;
			stop = true;
		}


        try {
            interrupt();
            this.join();
        } catch (InterruptedException e) {

        }
    }
	public void run(){
		while (true){
			try {
				Map<String,Map<String, TimeMetric>> timeMetrics = queue.poll(10l, TimeUnit.SECONDS);
				if(timeMetrics != null) {
					keyTimeMetrics.persistentOlder(timeMetrics);
				}
				else {
					if (stop) {
						break;
					}
				}
			} catch (InterruptedException e) {
				break;
			}
		}
	}
}
