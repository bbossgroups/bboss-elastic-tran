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
import org.frameworkset.tran.metrics.entity.TimeMetric;
import org.frameworkset.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>Description: Time-Key指标计算模型</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/5/28 20:42
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class TimeKeyMetrics implements BaseMetrics {
	private String metricsName = "TimeKeyMetrics";

	/**
	 * s0和s1区域大小
	 */
	protected int segmentBoundSize = 100000;

	public void setSegmentBoundSize(int segmentBoundSize) {
		this.segmentBoundSize = segmentBoundSize;
	}



	/**
	 * 指标时间key对应的指标数据
	 */


	private Map<String,TimeMetricHolder> timeMetricHolders = new LinkedHashMap<>();
	private ReentrantLock write = new ReentrantLock();






	private static Logger logger = LoggerFactory.getLogger(TimeKeyMetrics.class);






//	/**
//	 * 定时存储指标表数据
//	 * @param metrics
//	 */
//	protected abstract void persistentKeyMetrics(Map<String,KeyMetric> metrics);

	private TimeMetricsScanTask metricsThread;
	private MetricsConfig metricsConfig;
	private KeyMetricsPersistent metricsPersistent;

	public int getTimeWindowType() {
		return timeWindowType;
	}

	public void setTimeWindowType(int timeWindowType) {
		this.timeWindowType = timeWindowType;
	}

	private int timeWindowType = MetricsConfig.TIME_WINDOW_TYPE_MINUTE;
	/**
	 * 持久化数据扫描时间间隔，单位:毫秒
	 */
	private long scanInterval = 5000L;
	/**
	 * 持久化数据驻留时间窗口，单位：秒，默认5分钟
	 */
	private int timeWindows = 60;
	/**
	 * 设置持久化数据扫描窗口，单位：秒
	 */
	public void setTimeWindows(int timeWindows) {
		this.timeWindows = timeWindows;
	}
	/**
	 * 设置持久化数据扫描时间间隔，单位:毫秒
	 */
	public void setScanInterval(long scanInterval) {
		this.scanInterval = scanInterval;
	}

	public void init(){
		MetricsConfig metricsConfig = new MetricsConfig();
		metricsConfig.setScanInterval(scanInterval);
		if(timeWindows > 0)
			metricsConfig.setTimeWindows(0-timeWindows);
		else if(timeWindows < 0){
			metricsConfig.setTimeWindows(timeWindows);
		}
		else{
			metricsConfig.setTimeWindows(-300);
		}
		this.metricsConfig = metricsConfig;
		KeyMetricsPersistent metricsPersistent = new KeyMetricsPersistent("TimeKeyMetrics");
		metricsPersistent.setPersistent(this);
		metricsPersistent.init();
		this.metricsPersistent = metricsPersistent;

		TimeMetricsScanTask scanTask = new TimeMetricsScanTask("ScanTask-"+metricsName);
		scanTask.setMetricsConfig(metricsConfig);
		scanTask.setMetrics(this);
		scanTask.start();
		metricsThread = scanTask;
//		ShutdownUtil.addShutdownHook(new Runnable() {
//			@Override
//			public void run() {
//				stopMetrics();
//			}
//		});
	}

	@Override
	public void setMetricsName(String metricsName) {
		this.metricsName = metricsName;
	}

	private boolean needPersistent(Date slot, KeyMetric metric){
		return metric.getSlotTime().before(slot);
	}


	public DateFormat getMetricsTimeKeyFormat(MapData data){
		if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_MINUTE) {
			return data.getMinuteFormat();
		}
		else if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_SECOND)
			return data.getSecondFormat();
		else if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_HOUR)
			return data.getHourFormat();
		else if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_DAY)
			return data.getDayFormat();
		else if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_WEEK)
			return data.getWeekFormat();
		else if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_MONTH)
			return data.getMonthFormat();
		return data.getMinuteFormat();
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

		if(metricsThread != null){

			metricsThread.stopScan();


		}
		stopTimeMetricHolder();
		if(metricsPersistent != null)
			metricsPersistent.stop();
	}

	/**
	 * 强制执行所有指标数据持久化操作
	 */
    @Override
	public void forceFlush(boolean clearMetricKey,boolean waitComplete){
//		final Map<String,Map<String,TimeMetric>>  persistentData = new HashMap<String,Map<String,TimeMetric>>();
		long s = System.currentTimeMillis();
		logger.info("Force Flush timemetrics start.");
		List<String> emptyHolder = new ArrayList<>();
        List<Future> futures = new ArrayList<>();
		write.lock();
		try {

			Iterator<Map.Entry<String, TimeMetricHolder>> iterator = timeMetricHolders.entrySet().iterator();
			while (iterator.hasNext())
			{
				TimeMetricHolder timeMetricHolder = iterator.next().getValue();
				String metricsTimeKey = timeMetricHolder.getMetricTimeKey();
				List<Future> temp = timeMetricHolder.forceFlush(waitComplete);
                if(waitComplete && temp.size() > 0 ){
                    futures.addAll(temp);
                }
				if(timeMetricHolder.isEmpty()){
					emptyHolder.add(metricsTimeKey);
				}

			}
			if(emptyHolder.size() > 0){//清理空过期holder
				emptyHolder.forEach(timeMetricHolder -> {
					timeMetricHolders.remove(timeMetricHolder);
				});
			}
		}
		finally {
			write.unlock();
		}
        if(waitComplete && futures.size() > 0){
            for(Future future:futures){
                try {
                    future.get();
                } catch (InterruptedException e) {

                } catch (ExecutionException e) {
                    logger.error("",e);
                }
            }
        }
		logger.info("Force Flush timekeymetrics complete elapse:{} ms.",System.currentTimeMillis() - s);


	}

	/**
	 * 强制执行所有指标数据持久化操作
	 */
	private void stopTimeMetricHolder(){
		write.lock();
		try {

			Iterator<Map.Entry<String, TimeMetricHolder>> iterator = timeMetricHolders.entrySet().iterator();
			while (iterator.hasNext())			{
				TimeMetricHolder timeMetricHolder = iterator.next().getValue();
				timeMetricHolder.stopMetrics();
			}

			timeMetricHolders.clear();

		}
		finally {
			write.unlock();
		}


	}
	public TimeMetric metric(String metricsKey, MapData data, KeyMetricBuilder metricBuilder)  {
		if(!metricBuilder.validateData( data)){
			if(logger.isDebugEnabled())
				logger.debug("data validate failed:{}", SimpleStringUtil.object2json(data.getData()));
			return null;
		}
//		Map<String,TimeMetric> metrics = getTimeMetrics( metricsKey);
		Date time = data.metricsDataTime(metricsKey);
		DateFormat dateFormat = this.getMetricsTimeKeyFormat(data);//data.getMinuteFormat();
//		DateFormat metricTimeDateFormat = dateFormat;
//		String metricsTime = dateFormat.format(time);
		Date slotTime = new Date();
		String metricsTimeKey = dateFormat.format(time);
		String metricsSlotTime = dateFormat.format(slotTime);
		TimeMetricHolder timeMetricHolder = null;
		write.lock();
		try {

			timeMetricHolder = timeMetricHolders.get(metricsTimeKey);
			if (timeMetricHolder == null) {
				timeMetricHolder = new TimeMetricHolder();
				timeMetricHolder.setMetricsSlotTimeKey(metricsSlotTime);
				timeMetricHolder.setMetricsPersistent(metricsPersistent);
				timeMetricHolder.setTimeMetrics(this);
				timeMetricHolder.setSlotTime(slotTime);
				timeMetricHolder.setSegmentBoundSize(segmentBoundSize);
				timeMetricHolder.setMetricTimeKey(metricsTimeKey);
				timeMetricHolder.init();
				timeMetricHolders.put(metricsTimeKey, timeMetricHolder);
			}
			TimeMetric metric = (TimeMetric) timeMetricHolder.metric(metricsKey, data, metricBuilder);
			return metric;
		}
		finally {
			write.unlock();
		}





	}
	private void _persistent(Collection<KeyMetric> keyMetrics){
		metricsPersistent.persistent(keyMetrics);
	}
	/**
	 * 扫描达到时间窗口阈值的指标
	 */
	public void scanPersistentMetrics(){
		Date slot = TimeUtil.addDateSeconds(new Date(),metricsConfig.getTimeWindows());
//		final Map<String,Map<String,TimeMetric>>  persistentData = new HashMap<String,Map<String,TimeMetric>>();
		PersistentDataHolder persistentData = new PersistentDataHolder();
		persistentData.init();
		List<String> emptyHolder = new ArrayList<>();
		write.lock();
		try {

			Iterator<Map.Entry<String, TimeMetricHolder>> iterator = timeMetricHolders.entrySet().iterator();
			while (iterator.hasNext())
			{
				TimeMetricHolder timeMetricHolder = iterator.next().getValue();
				String metricsTimeKey = timeMetricHolder.getMetricTimeKey();

				if(timeMetricHolder.needPersistent(slot)){

					if(!timeMetricHolder.isTimeWindowOlder(slot)) {
						timeMetricHolder.scanPersistentMetrics(new PersistentScanCallback() {

							@Override
							public boolean scanTimeMetric2Persistent(KeyMetric keyMetric) {
								if (needPersistent(slot, keyMetric)) {
									persistentData.addKeyMetric(keyMetric);
//									if (persistentBatchsize > 0 && persistentData.size() >= persistentBatchsize) {//分批分段处理数据
//										_persistent(persistentData.getPersistentData());
//										persistentData.init();
//									}
									return true;
								}
								return false;
							}
						});
						if (timeMetricHolder.isEmpty()) {
							emptyHolder.add(metricsTimeKey);
						}
					}
					else{//整个操已经到期，全部持久化
						timeMetricHolder.persisteMetrics();
						emptyHolder.add(metricsTimeKey);
					}

				}
				else{ // 由于时间顺序性，所以没有达到时间窗口的后续无需检测
					break;
				}

			}
			if(persistentData != null && persistentData.size() > 0){
				_persistent(persistentData.getPersistentData());
				persistentData.clear();
			}
			if(emptyHolder.size() > 0){//清理空过期holder
				emptyHolder.forEach(timeMetricHolder -> {
					timeMetricHolders.remove(timeMetricHolder);
				});
			}
		}
		finally {
			write.unlock();
		}



	}


}
