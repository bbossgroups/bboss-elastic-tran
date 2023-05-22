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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/5/29 15:49
 * @author biaoping.yin
 * @version 1.0
 */
public class TimeMetricsScanTask extends Thread{
	private static Logger logger = LoggerFactory.getLogger(TimeMetricsScanTask.class);
	private MetricsConfig metricsConfig;
	private TimeKeyMetrics metrics;
	private boolean stoped;
	public TimeMetricsScanTask(String tname){
		super(tname);
	}


	//	/**
//	 * 强制将指标持久化
//	 */
//	public synchronized void forceFlush(boolean cleanMetricsKey){
//		Map<String, Map<String,TimeMetric>> persistentMetrics = metrics.scanPersistentMetrics(true);
//		if(persistentMetrics.size() > 0)
//			metricsPersistent.persistent(metrics,persistentMetrics);
//		if(cleanMetricsKey){
//			metrics.cleanMetrics();
//		}
//	}
    private boolean started;
    public void start(){
        super.start();
        started = true;
    }
    private Object stopScanLock = new Object();

	public   void stopScan(){
        if (!started || stoped)
            return;
		synchronized (stopScanLock) {
			if (stoped)
				return;
			stoped = true;
		}

		this.interrupt();
        try {
            this.join();
        } catch (InterruptedException e) {

        }
    }
	public void run(){
		while(true){
			if(stoped) {
				break;
			}
			try {

				sleep(metricsConfig.getScanInterval());

			} catch (InterruptedException e) {

				break;
			}
			if(stoped) {
				break;
			}

			metrics.scanPersistentMetrics();

		}


	}

	public void setMetricsConfig(MetricsConfig metricsConfig) {
		this.metricsConfig = metricsConfig;
	}



	public void setMetrics(TimeKeyMetrics metrics) {
		this.metrics = metrics;
	}
}
