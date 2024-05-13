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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/5/28 20:42
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class KeyMetrics extends BaseKeyMetrics implements BaseMetrics {
	private String metricsName = "TimeMetrics";
	private ReentrantLock lock = new ReentrantLock();
	private static Logger logger = LoggerFactory.getLogger(KeyMetrics.class);
	protected void initKeyMetric(KeyMetric keyMetric, MapData data, String metricsKey){
		keyMetric.setDataTime(data.metricsDataTime(metricsKey));
		keyMetric.setSlotTime(new Date());
		keyMetric.setMetric(metricsKey);
		keyMetric.init(data);
	}

	@Override
	public void setMetricsName(String metricsName) {
		this.metricsName = metricsName;
	}

	public KeyMetric metric(String metricsKey, MapData data, KeyMetricBuilder metricBuilder)  {
		if(!metricBuilder.validateData( data)){
			if(logger.isDebugEnabled())
				logger.debug("data validate failed:{}", SimpleStringUtil.object2json(data.getData()));
			return null;
		}
		boolean isFull = false;
		KeyMetric keyMetric = null;
		KeyMetricsContainer keyMetricsContainerTemp = keyMetricsContainerS0;
		KeyMetricsContainer persistent = null;
		lock.lock();
		try {

			keyMetric = keyMetricsContainerTemp.getKeyMetric(metricsKey);
			if(keyMetric == null){
				keyMetric = keyMetricsContainerS1.getKeyMetric(metricsKey);
			}
			if(keyMetric == null){
				keyMetric =  metricBuilder.build();

				initKeyMetric(keyMetric,data,metricsKey);

				isFull = !keyMetricsContainerTemp.putKeyMetric(metricsKey,keyMetric);
				if(isFull){
					if(keyMetricsContainerS1.isEmpty()) {//交换分区s0和s1
						keyMetricsContainerS0 = keyMetricsContainerS1;
						keyMetricsContainerS1 = keyMetricsContainerTemp;
					}
					else{
						persistent = keyMetricsContainerS1;
						keyMetricsContainerS1 = keyMetricsContainerTemp;
						KeyMetricsContainer keyMetricsContainerS0Temp = buildKeyMetricsContainer();
						keyMetricsContainerS0 = keyMetricsContainerS0Temp;
					}
				}

			}


		}
		finally {
			lock.unlock();
		}
		keyMetric.increment(data);
		if(persistent != null){
			persistent(persistent);
		}


		return keyMetric;

	}







	@Override
	protected void initMetrics(){

		KeyMetricsPersistent metricsPersistent = new KeyMetricsPersistent();
		metricsPersistent.setPersistent(this);
		metricsPersistent.init();
		this.setMetricsPersistent(metricsPersistent);

//		ShutdownUtil.addShutdownHook(new Runnable() {
//			@Override
//			public void run() {
//				stopMetrics();
//			}
//		});
	}
	/**
	 * 强制执行所有指标数据持久化操作
	 */
    @Override
	public void forceFlush(boolean clearMetricsKey,boolean waitComplete){
        long s = System.currentTimeMillis();
        Future future = null;
        lock.lock();
		try {

			if (!keyMetricsContainerS0.isEmpty()) {
                 future = persistent(keyMetricsContainerS0);
				keyMetricsContainerS0 = buildKeyMetricsContainer();
			}

			if (!keyMetricsContainerS1.isEmpty()) {
                 future = persistent(keyMetricsContainerS1);
				keyMetricsContainerS1 = buildKeyMetricsContainer();
			}
		}
		finally {
			lock.unlock();
		}
        if(waitComplete && future != null){
            try {
                future.get();
            } catch (InterruptedException e) {

            } catch (ExecutionException e) {
                logger.error("",e);
            }
        }
        logger.info("Force Flush keymetrics complete elapse:{} ms.",System.currentTimeMillis() - s);

	}

    private boolean stoped;
    private Object stopLock = new Object();
    @Override
    public void stopMetrics(){
        if(stoped){
            return;
        }
        synchronized (stopLock){
            if(stoped)
                return;
            stoped = true;
        }
		forceFlush(true,true);
        if(metricsPersistent != null)
            metricsPersistent.stop();
	}




}
