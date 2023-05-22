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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/5/28 20:42
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class TimeMetrics implements BaseMetrics{
	private String metricsName = "TimeMetrics";
	private ReadWriteLock lock = new ReentrantReadWriteLock();
	private Lock read = lock.readLock();
	private Lock write = lock.writeLock();

	public String getMetricsName() {
		return metricsName;
	}

	public void setMetricsName(String metricsName) {
		this.metricsName = metricsName;
	}

	/**
	 * 用来保存基于时间窗口各个指标的统计数据
	 *  Map<指标,Map<分钟,Metric>> metrics
	 */
	private Map<String,Map<String, TimeMetric>> timeMetrics = new HashMap<String, Map<String, TimeMetric>>();
	public Map<String, TimeMetric> getTimeKeyMetrics(String metricsKey){
		Map<String, TimeMetric> metric = timeMetrics.get(metricsKey);
		if(metric != null)
			return metric;
		write.lock();
		try {

			metric = timeMetrics.get(metricsKey);
			if(metric == null) {
				metric = new LinkedHashMap<>();
				timeMetrics.put(metricsKey, metric);
			}
		}
		finally {
			write.unlock();
		}
		return metric;
	}


	private boolean needPersistent(Date slot, TimeMetric metric){
		return metric.getSlotTime().before(slot);
	}


	public List<KeyMetric> scanPersistentMetrics(){
		Date slot = TimeUtil.addDateSeconds(new Date(),metricsConfig.getTimeWindows());
		final List<KeyMetric>  persistentData = new ArrayList<>();
		read.lock();
		try {

			timeMetrics.entrySet().forEach((entry) -> {
				final String metricsKey = entry.getKey();
				final Map<String, TimeMetric> metricMap = entry.getValue();
				synchronized (metricMap) {
					List<String> metricKeys = new ArrayList<String>();
					Iterator<Map.Entry<String,TimeMetric>> iterator = metricMap.entrySet().iterator();
					while(iterator.hasNext()) {
						Map.Entry<String,TimeMetric> minutesMetric = iterator.next();
						TimeMetric metric = minutesMetric.getValue();
						if (needPersistent(slot, metric)) {
							metricKeys.add(minutesMetric.getKey());
							persistentData.add(metric);
						}
						else{
							break;
						}
					}
					if (metricKeys.size() > 0) {
						for (String key : metricKeys) {
							metricMap.remove(key);
						}
					}
				}
			});
		}
		finally {
			read.unlock();
		}
		return persistentData;
	}

	private static Logger logger = LoggerFactory.getLogger(TimeMetrics.class);
	public DateFormat getMetricsTimeKeyFormat(MapData data){
		if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_MINUTE)
			return data.getMinuteFormat();
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
	public TimeMetric metric(String metricsKey, MapData data, KeyMetricBuilder metricBuilder)  {
		if(!metricBuilder.validateData( data)){
			if(logger.isDebugEnabled())
				logger.debug("data validate failed:{}", SimpleStringUtil.object2json(data.getData()));
			return null;
		}
		Map<String, TimeMetric> metrics = getTimeKeyMetrics( metricsKey);
		Date time = data.metricsDataTime(metricsKey);
		DateFormat dateFormat = this.getMetricsTimeKeyFormat(data);//data.getMinuteFormat();
		DateFormat metricTimeDateFormat = dateFormat;
		String metricsTime = dateFormat.format(time);
		TimeMetric metric = null;
		synchronized (metrics) {
			metric = metrics.get(metricsTime);
			if (metric == null) {
				metric = (TimeMetric)metricBuilder.build();

				dateFormat = data.getDayFormat();
				String day = dateFormat.format(time);
				dateFormat = data.getHourFormat();
				String hour = dateFormat.format(time);
				dateFormat = data.getMinuteFormat();
				String min = dateFormat.format(time);

				dateFormat = data.getMonthFormat();
				if(dateFormat != null) {
					String month = dateFormat.format(time);
					metric.setMonth(month);
				}
				dateFormat = data.getWeekFormat();
				if(dateFormat != null) {
					String week = dateFormat.format(time);
					metric.setWeek(week);
				}
				dateFormat = data.getYearFormat();
				if(dateFormat != null) {
					String year = dateFormat.format(time);
					metric.setYear(year);
				}
				metric.setDay(day);
				metric.setDayHour(hour);
				metric.setMinute(min);
//				metric.setMiniute(metricsTime);
				metric.setMetric(metricsKey);
				metric.setSlotTime(new Date());
				metric.setMetricTimeKey(metricsTime);
				metric.setMetricSlotTimeKey(data.getMinuteFormat().format(metric.getSlotTime()));
				try {
					metric.setDataTime(metricTimeDateFormat.parse(metricsTime));
				} catch (Exception e) {
					logger.error("设置指标时间异常",e);
				}
				metric.init(data);

				metrics.put(metricsTime, metric);
			}
			metric.increment(data);
		}
		return metric;

	}


	private MetricsThread metricsThread;
	private KeyMetricsPersistent metricsPersistent;
	private MetricsConfig metricsConfig;

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
	private long scanInterval = 5000l;
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
	private int persistentDataHolderSize = 5000;

	public void setPersistentDataHolderSize(int persistentDataHolderSize) {
		this.persistentDataHolderSize = persistentDataHolderSize;
	}

	/**
	 * 强制将指标持久化
	 */
    @Override
	public void forceFlush(boolean cleanMetricsKey,boolean waitComplete){
		final PersistentDataHolder persistentDataHolder = new PersistentDataHolder();
		persistentDataHolder.init();
        List<Future> futures = new ArrayList<>();
        long s = System.currentTimeMillis();
		if(!cleanMetricsKey) {
			read.lock();
			try {

				timeMetrics.entrySet().forEach((entry) -> {
					final Map<String, TimeMetric> metricMap = entry.getValue();
					synchronized (metricMap) {
						Iterator<Map.Entry<String, TimeMetric>> iterator = metricMap.entrySet().iterator();
						while (iterator.hasNext()) {
							Map.Entry<String, TimeMetric> minutesMetric = iterator.next();
							TimeMetric metric = minutesMetric.getValue();
							persistentDataHolder.addKeyMetric(metric);
							if (persistentDataHolder.size() >= persistentDataHolderSize) {
                                Future future = metricsPersistent.persistent(persistentDataHolder.getPersistentData());
                                if(waitComplete){
                                    futures.add(future);
                                }
								persistentDataHolder.init();
							}

						}
						metricMap.clear();
					}
				});

			} finally {
				read.unlock();
			}
		}
		else{
			write.lock();
			try {

				timeMetrics.entrySet().forEach((entry) -> {
					final Map<String, TimeMetric> metricMap = entry.getValue();
					synchronized (metricMap) {
						Iterator<Map.Entry<String, TimeMetric>> iterator = metricMap.entrySet().iterator();
						while (iterator.hasNext()) {
							Map.Entry<String, TimeMetric> minutesMetric = iterator.next();
							TimeMetric metric = minutesMetric.getValue();
							persistentDataHolder.addKeyMetric(metric);
							if (persistentDataHolder.size() >= persistentDataHolderSize) {
                                Future future = metricsPersistent.persistent(persistentDataHolder.getPersistentData());
                                if(waitComplete){
                                    futures.add(future);
                                }
								persistentDataHolder.init();
							}

						}
						metricMap.clear();
					}
				});
				timeMetrics.clear();

			} finally {
				write.unlock();
			}
		}
		if(persistentDataHolder.size() > 0) {
            Future future = metricsPersistent.persistent(persistentDataHolder.getPersistentData());
            if(waitComplete){
                futures.add(future);
            }
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
        logger.info("Force Flush timemetrics complete elapse:{} ms.",System.currentTimeMillis() - s);

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
		KeyMetricsPersistent metricsPersistent = new KeyMetricsPersistent();
		metricsPersistent.setPersistent(this);
		metricsPersistent.init();
		this.metricsPersistent = metricsPersistent;
		ScanTask scanTask = new ScanTask();
		scanTask.setMetricsConfig(metricsConfig);
		scanTask.setMetricsPersistent(metricsPersistent);
		scanTask.setMetrics(this);
		metricsThread = new MetricsThread(scanTask,"ScanTask-"+metricsName);
		metricsThread.start();
//		ShutdownUtil.addShutdownHook(new Runnable() {
//			@Override
//			public void run() {
//				stopMetrics();
//			}
//		});
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
			forceFlush(true,true);
            this.metricsPersistent.stop();
		}
	}



}
