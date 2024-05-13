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
 * <p>Description: Key-Time指标计算模型</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/5/28 20:42
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class KeyTimeMetrics implements BaseMetrics {
	private String metricsName = "KeyTimeMetrics";
	private ReadWriteLock lock = new ReentrantReadWriteLock();
	private Lock read = lock.readLock();
	private Lock write = lock.writeLock();
	private FlushOlder flushOlder;
	/**
	 * 新区和老区大小：做多存放指标个数
	 */
	private int segmentBoundSize = 10000000;

	public void setSegmentBoundSize(int segmentBoundSize) {
		this.segmentBoundSize = segmentBoundSize;
	}

	public int getSegmentBoundSize() {
		return segmentBoundSize;
	}

	/**
	 * 用来保存基于时间窗口各个指标的统计数据
	 *  Map<指标,Map<分钟,Metric>> metrics
	 */
	//新区
	private Map<String,Map<String, TimeMetric>> timeMetrics0 ;
	//老区
	private Map<String,Map<String, TimeMetric>> timeMetrics1 ;
	public Map<String, TimeMetric> getTimeKeyMetrics(String metricsKey){
		Map<String, TimeMetric> metric = timeMetrics1.get(metricsKey);//老区
		if(metric == null){
			metric = timeMetrics0.get(metricsKey);//新区
		}
		if(metric != null)
			return metric;
		Map<String,Map<String, TimeMetric>> timeMetrics0Temp = null;
		write.lock();
		try {


			metric = timeMetrics1.get(metricsKey);//老区
			if(metric == null){
				metric = timeMetrics0.get(metricsKey);//新区
			}
			if(metric == null) {
				if(timeMetrics0.size() >= segmentBoundSize){
					if(timeMetrics1.isEmpty()) {
						timeMetrics0Temp = timeMetrics0;
						timeMetrics0 = timeMetrics1;
						timeMetrics1 = timeMetrics0Temp;
						timeMetrics0Temp = null;
					}
					else{
						timeMetrics0Temp = timeMetrics1;//释放老区
						timeMetrics1 = timeMetrics0;//将新区设置到老区
						timeMetrics0 = new HashMap<String, Map<String, TimeMetric>>();//初始化新区
					}


				}
				metric = new LinkedHashMap<>();
				timeMetrics0.put(metricsKey, metric);
			}
		}
		finally {
			write.unlock();
		}
		if(timeMetrics0Temp != null && timeMetrics0Temp.size() > 0){
			logger.info("timeMetrics0 and timeMetrics1 is full, force flush timeMetrics1");
			flushOlder.add(timeMetrics0Temp);
		}
		return metric;
	}
	private int persistentDataHolderSize = 5000;

	public void setPersistentDataHolderSize(int persistentDataHolderSize) {
		this.persistentDataHolderSize = persistentDataHolderSize;
	}

	public void persistentOlder(Map<String,Map<String, TimeMetric>> timeMetricsOlder){
		PersistentDataHolder persistentDataHolder = new PersistentDataHolder();
		persistentDataHolder.init();
		timeMetricsOlder.entrySet().forEach((entry) -> {
				final Map<String, TimeMetric> metricMap = entry.getValue();
				synchronized (metricMap) {
					Iterator<Map.Entry<String, TimeMetric>> iterator = metricMap.entrySet().iterator();
					while (iterator.hasNext()) {
						Map.Entry<String, TimeMetric> minutesMetric = iterator.next();
						TimeMetric metric = minutesMetric.getValue();
						persistentDataHolder.addKeyMetric(metric);
						if(persistentDataHolder.size() >= persistentDataHolderSize ){
							metricsPersistent.persistent( persistentDataHolder.getPersistentData());
							persistentDataHolder.init();
						}

					}
					metricMap.clear();
				}
			});
		timeMetricsOlder.clear();
		if(persistentDataHolder.size() > 0) {
			metricsPersistent.persistent(persistentDataHolder.getPersistentData());
			persistentDataHolder.clear();
		}
	}

	private boolean needPersistent(Date slot, TimeMetric metric){
		return metric.getSlotTime().before(slot);
	}









	public Collection<KeyMetric> scanPersistentMetrics(){
		Date slot = TimeUtil.addDateSeconds(new Date(),metricsConfig.getTimeWindows());
		final List<KeyMetric>  persistentData = new ArrayList<>();
		read.lock();
		try {

			if(timeMetrics0.size() > 0) {
				_scanPersistentMetrics(persistentData, slot, timeMetrics0);
			}
			if(timeMetrics1.size() > 0) {
				_scanPersistentMetrics(persistentData, slot, timeMetrics1);
			}

		}
		finally {
			read.unlock();
		}
		return persistentData;
	}

	private void _scanPersistentMetrics(final List<KeyMetric>  persistentData,Date slot,Map<String,Map<String, TimeMetric>> timeMetrics){
		timeMetrics.entrySet().forEach((entry) -> {
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
//							topersistent(metricsKey, persistentData, minutesMetric.getKey(), metric);
					}
					else{//数据的顺序性，后续无需扫描
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

	private static Logger logger = LoggerFactory.getLogger(KeyTimeMetrics.class);
	public DateFormat getMetricsTimeKeyFormat(MapData data){
//		if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_MINUTE)
//			return data.getMinuteFormat();
//		else if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_SECOND)
//			return data.getSecondFormat();
//		else if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_HOUR)
//			return data.getHourFormat();
//		else if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_DAY)
//			return data.getDayFormat();
//		else if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_WEEK)
//			return data.getWeekFormat();
//		else if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_MONTH)
//			return data.getMonthFormat();
//        else if(this.timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_YEAR)
//            return data.getYearFormat();
//		return data.getMinuteFormat();
        return MetricUtil.getMetricsTimeKeyFormat(this.timeWindowType,data);
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
                MetricUtil.buildMetricTimeField( metric,  data,  time);

				metric.setMetric(metricsKey);
				try {
					metric.setDataTime(metricTimeDateFormat.parse(metricsTime));
				} catch (Exception e) {
					logger.error("设置指标时间异常",e);
				}


				metric.setSlotTime(new Date());
				metric.setMetricTimeKey(metricsTime);
//				metric.setMetricSlotTimeKey(MetricsConfig.getMinuteFormat().format(metric.getSlotTime()));
				metric.init(data);				
				metrics.put(metricsTime, metric);
			}
			metric.increment(data);
		}

		return metric;

	}


	private KeyTimeScanTask metricsThread;
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

	public void init(){
		timeMetrics0 = new HashMap<String, Map<String, TimeMetric>>();
		timeMetrics1 = new HashMap<String, Map<String, TimeMetric>>();
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
		KeyTimeScanTask scanTask = new KeyTimeScanTask("KeyTimeScanTask-"+metricsName);
		scanTask.setMetricsConfig(metricsConfig);
		scanTask.setMetricsPersistent(metricsPersistent);
		scanTask.setMetrics(this);
		scanTask.start();
		metricsThread = scanTask;
//		ShutdownUtil.addShutdownHook(new Runnable() {
//			@Override
//			public void run() {
//				stopMetrics();
//			}
//		});
		flushOlder = new FlushOlder();
		flushOlder.setKeyTimeMetrics(this);
		flushOlder.start();
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
			flushOlder.stopFlush();
			forceFlush(true,true);
//            this.metricsPersistent.stop();
		}
        if(metricsPersistent != null)
            metricsPersistent.stop();
	}

	public List<KeyMetric> getAll(){
		final List<KeyMetric>  persistentData = new ArrayList<>();
		read.lock();
		try {

			timeMetrics0.entrySet().forEach((entry) -> {
				final Map<String, TimeMetric> metricMap = entry.getValue();
				synchronized (metricMap) {
					Iterator<Map.Entry<String,TimeMetric>> iterator = metricMap.entrySet().iterator();
					while(iterator.hasNext()) {
						Map.Entry<String,TimeMetric> minutesMetric = iterator.next();
						TimeMetric metric = minutesMetric.getValue();
						persistentData.add(metric);

					}
					metricMap.clear();
				}
			});
			timeMetrics1.entrySet().forEach((entry) -> {
				final Map<String, TimeMetric> metricMap = entry.getValue();
				synchronized (metricMap) {
					Iterator<Map.Entry<String,TimeMetric>> iterator = metricMap.entrySet().iterator();
					while(iterator.hasNext()) {
						Map.Entry<String,TimeMetric> minutesMetric = iterator.next();
						TimeMetric metric = minutesMetric.getValue();
						persistentData.add(metric);

					}
					metricMap.clear();
				}
			});
		}
		finally {
			read.unlock();
		}
		return persistentData;
	}

	/**
	 * 强制将指标持久化
	 */
    @Override
	public  void forceFlush(boolean cleanMetricsKey,boolean waitComplete) {
		logger.info("Force Flush timemetrics start.waitComplete:{},cleanMetricsKey:{}",waitComplete,cleanMetricsKey);
		long s = System.currentTimeMillis();
		PersistentDataHolder  persistentData = new PersistentDataHolder();
		persistentData.init();
        List<Future> futures = new ArrayList<>();

		if(!cleanMetricsKey) {
			read.lock();
			try {


				timeMetrics0.entrySet().forEach((entry) -> {
					final Map<String, TimeMetric> metricMap = entry.getValue();
					synchronized (metricMap) {
						Iterator<Map.Entry<String, TimeMetric>> iterator = metricMap.entrySet().iterator();
						while (iterator.hasNext()) {
							Map.Entry<String, TimeMetric> minutesMetric = iterator.next();
							TimeMetric metric = minutesMetric.getValue();
							persistentData.addKeyMetric(metric);
							if (persistentData.size() >= persistentDataHolderSize) {
								Future future = this.metricsPersistent.persistent(persistentData.getPersistentData());
                                if(waitComplete){
                                    futures.add(future);
                                }
								persistentData.init();
							}
						}
						metricMap.clear();
					}
				});
				timeMetrics1.entrySet().forEach((entry) -> {
					final Map<String, TimeMetric> metricMap = entry.getValue();
					synchronized (metricMap) {
						Iterator<Map.Entry<String, TimeMetric>> iterator = metricMap.entrySet().iterator();
						while (iterator.hasNext()) {
							Map.Entry<String, TimeMetric> minutesMetric = iterator.next();
							TimeMetric metric = minutesMetric.getValue();
							persistentData.addKeyMetric(metric);
							if (persistentData.size() >= persistentDataHolderSize) {
                                Future future = this.metricsPersistent.persistent(persistentData.getPersistentData());
                                if(waitComplete){
                                    futures.add(future);
                                }
								persistentData.init();
							}
						}
						metricMap.clear();
					}
				});

				if (persistentData.size() > 0) {
                    Future future = this.metricsPersistent.persistent(persistentData.getPersistentData());
                    if(waitComplete){
                        futures.add(future);
                    }
					persistentData.clear();
				}


			} finally {
				read.unlock();

			}
		}
		else{
			write.lock();
			try {


				timeMetrics0.entrySet().forEach((entry) -> {
					final Map<String, TimeMetric> metricMap = entry.getValue();
					synchronized (metricMap) {
						Iterator<Map.Entry<String, TimeMetric>> iterator = metricMap.entrySet().iterator();
						while (iterator.hasNext()) {
							Map.Entry<String, TimeMetric> minutesMetric = iterator.next();
							TimeMetric metric = minutesMetric.getValue();
							persistentData.addKeyMetric(metric);
							if (persistentData.size() >= persistentDataHolderSize) {
                                Future future = this.metricsPersistent.persistent(persistentData.getPersistentData());
                                if(waitComplete){
                                    futures.add(future);
                                }
								persistentData.init();
							}
						}
						metricMap.clear();
					}
				});
				timeMetrics1.entrySet().forEach((entry) -> {
					final Map<String, TimeMetric> metricMap = entry.getValue();
					synchronized (metricMap) {
						Iterator<Map.Entry<String, TimeMetric>> iterator = metricMap.entrySet().iterator();
						while (iterator.hasNext()) {
							Map.Entry<String, TimeMetric> minutesMetric = iterator.next();
							TimeMetric metric = minutesMetric.getValue();
							persistentData.addKeyMetric(metric);
							if (persistentData.size() >= persistentDataHolderSize) {
                                Future future = this.metricsPersistent.persistent(persistentData.getPersistentData());
                                if(waitComplete){
                                    futures.add(future);
                                }
								persistentData.init();
							}
						}
						metricMap.clear();
					}
				});

				if (persistentData.size() > 0) {
                    Future future = this.metricsPersistent.persistent(persistentData.getPersistentData());
                    if(waitComplete){
                        futures.add(future);
                    }
					persistentData.clear();
				}
				timeMetrics0.clear();
				timeMetrics1.clear();

			} finally {
				write.unlock();

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
		logger.info("Force Flush keytimemetrics complete elapse:{} ms.",System.currentTimeMillis() - s);
	}


	public String getMetricsName() {
		return metricsName;
	}

	public void setMetricsName(String metricsName) {
		this.metricsName = metricsName;
	}
}
