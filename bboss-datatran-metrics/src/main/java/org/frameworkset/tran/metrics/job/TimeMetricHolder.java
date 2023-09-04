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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.TimeMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/8/20
 * @author biaoping.yin
 * @version 1.0
 */
public class TimeMetricHolder extends BaseKeyMetrics{
	private static Logger logger = LoggerFactory.getLogger(TimeKeyMetrics.class);
	private String metricsSlotTimeKey;
	private Date slotTime;
	private Date lastMetricSlotTime;
	private TimeKeyMetrics timeMetrics;
	public TimeMetricHolder(){
	}

	private String metricTimeKey;

	public void setMetricsSlotTimeKey(String metricsSlotTimeKey) {
		this.metricsSlotTimeKey = metricsSlotTimeKey;
	}

	public String getMetricsSlotTimeKey() {
		return metricsSlotTimeKey;
	}

	@Override
	protected void initMetrics(){

	}
	protected void initKeyMetric(KeyMetric metric,MapData data,String metricsKey){
		Date time = data.metricsDataTime(metricsKey);
		DateFormat metricsTimeKeyFormat = timeMetrics.getMetricsTimeKeyFormat(data);//data.getMinuteFormat();
		String timeMetricKey = metricsTimeKeyFormat.format(time);
        TimeMetric timeMetric = (TimeMetric)metric;
        MetricUtil.buildMetricTimeField( timeMetric,  data,  time);

		Date slotTime = new Date();
		lastMetricSlotTime = slotTime;
		timeMetric.setSlotTime(slotTime);
		timeMetric.setMetricTimeKey(timeMetricKey);
		timeMetric.setMetricSlotTimeKey(data.getMinuteFormat().format(slotTime));
//				metric.setMiniute(metricsTime);
		metric.setMetric(metricsKey);
		try {
			metric.setDataTime(metricsTimeKeyFormat.parse(timeMetricKey));
		} catch (Exception e) {
			logger.error("设置指标时间异常",e);
		}
		metric.init(data);
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
					keyMetricsContainerS0 = buildKeyMetricsContainer();
				}
			}

		}
		keyMetric.increment(data);

		if(persistent != null){
			persistent(persistent);
		}


		return keyMetric;

	}
	public String getMetricTimeKey() {
		return metricTimeKey;
	}
	public boolean needPersistent(Date slot){
		return getSlotTime().before(slot);
	}

	/**
	 * 持久化所有到期的指标
	 */
	public void persisteMetrics(){
		if (!keyMetricsContainerS0.isEmpty()) {
			persistent(keyMetricsContainerS0);
			keyMetricsContainerS0 = null;
		}

		if (!keyMetricsContainerS1.isEmpty()) {
			persistent(keyMetricsContainerS1);
			keyMetricsContainerS1 = null;
		}
	}

	public void stopMetrics(){
		persisteMetrics();
	}

	/**
	 * 强制执行所有指标数据持久化操作
	 */

	public List<Future> forceFlush(boolean waitComplete){
        Future future = null;
        List<Future> futures = new ArrayList<>();
        if (!keyMetricsContainerS0.isEmpty()) {
            future = persistent(keyMetricsContainerS0);
            if(waitComplete)
                futures.add(future);
            keyMetricsContainerS0 = buildKeyMetricsContainer();
        }

        if (!keyMetricsContainerS1.isEmpty()) {
            future = persistent(keyMetricsContainerS1);
            if(waitComplete)
                futures.add(future);
            keyMetricsContainerS1 = buildKeyMetricsContainer();
        }
        return futures;

	}

	public boolean isTimeWindowOlder(Date slotOldTime){
		if(lastMetricSlotTime.before(slotOldTime)){
			return true;
		}
		else{
			return false;
		}
	}
	public void scanPersistentMetrics(PersistentScanCallback persistentScanCallback){
		if(!keyMetricsContainerS1.isEmpty()) {
			keyMetricsContainerS1.scanPersistentMetrics(  persistentScanCallback);
		}
		if(!keyMetricsContainerS0.isEmpty()) {
			keyMetricsContainerS0.scanPersistentMetrics(  persistentScanCallback);
		}

	}

	public boolean isEmpty(){
		return keyMetricsContainerS0.isEmpty() && keyMetricsContainerS1.isEmpty();

	}


	public void setMetricTimeKey(String metricTimeKey) {
		this.metricTimeKey = metricTimeKey;
	}

	public void setSlotTime(Date slotTime) {
		this.slotTime = slotTime;
	}

	public Date getSlotTime() {
		return slotTime;
	}

	public void setTimeMetrics(TimeKeyMetrics timeMetrics) {
		this.timeMetrics = timeMetrics;
	}



}
