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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/5/29 15:49
 * @author biaoping.yin
 * @version 1.0
 */
public class KeyTimeScanTask extends Thread{
	private static Logger logger = LoggerFactory.getLogger(KeyTimeScanTask.class);
	private MetricsConfig metricsConfig;
	private KeyMetricsPersistent metricsPersistent;
	private KeyTimeMetrics metrics;
	private boolean stoped;
	public KeyTimeScanTask(String tname){
		super(tname);
	}

    private Object stopScanLock = new Object();
	public   void stopScan(){
		if(stoped )
			return;
        synchronized(stopScanLock) {
            if(stoped )
                return;
            stoped = true;
        }


        try {
            this.interrupt();
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
//				logger.error("ScanTask is Interrupted:",e);
			}
			if(stoped) {
				break;
			}

			Collection<KeyMetric> persistentMetrics = metrics.scanPersistentMetrics();
			if(persistentMetrics.size() > 0)
				metricsPersistent.persistent(persistentMetrics);

		}
//		Collection< KeyMetric> persistentMetrics = metrics.getTimeKeyMetrics();
//		if(persistentMetrics.size() > 0)
//			metricsPersistent.persistent(persistentMetrics);
//		metrics.cleanMetrics();

	}

	public void setMetricsConfig(MetricsConfig metricsConfig) {
		this.metricsConfig = metricsConfig;
	}

	public void setMetricsPersistent(KeyMetricsPersistent metricsPersistent) {
		this.metricsPersistent = metricsPersistent;
	}

	public void setMetrics(KeyTimeMetrics metrics) {
		this.metrics = metrics;
	}
}
