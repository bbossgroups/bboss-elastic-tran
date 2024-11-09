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

import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.MetricKey;
import org.frameworkset.tran.metrics.job.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/8/23
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class Metrics implements BaseMetrics {
    protected MetricsLogAPI metricsLogAPI;
	private BaseMetrics baseMetrics;
	private List<MetricBuilder> metricBuilders;
	private boolean inited;
	private String metricsName ;
	public static int MetricsType_KeyTimeMetircs = 0;
	public static int MetricsType_TimeKeyMetircs = 1;
	public static int MetricsType_TimeMetircs = 2;
	public static int MetricsType_KeyMetircs = 3;
	public static int MetricsType_DEFAULT = -1;
	private int metricsType = MetricsType_DEFAULT ;
    private static Logger logger = LoggerFactory.getLogger(Metrics.class);
	public Metrics(){
		metricsType = MetricsType_DEFAULT;
	}
	public Metrics(int metricsType){
		this.metricsType = metricsType;
	}
    protected MetricsLogAPI buildMetricsLogAPI(){
        return null;
    }

    public void setMetricsLogAPI(MetricsLogAPI metricsLogAPI) {
        this.metricsLogAPI = metricsLogAPI;
    }

    public MetricsLogAPI getMetricsLogAPI() {
        return metricsLogAPI;
    }
    /**
	 * 新区和老区大小：做多存放指标个数
	 */
	private int segmentBoundSize = 10000000;


	protected int timeWindowType = MetricsConfig.TIME_WINDOW_TYPE_MINUTE;
	/**
	 * 持久化数据扫描时间间隔，单位:毫秒
	 */
	private long scanInterval = 5000l;
	/**
	 * 持久化数据驻留时间窗口，单位：秒，默认1分钟
	 */
	private int timeWindows = 60;
	/**
	 * 设置持久化数据扫描窗口，单位：秒
	 */
	public void setTimeWindows(int timeWindows) {
		this.timeWindows = timeWindows;
	}

	public int getTimeWindowType() {
		return timeWindowType;
	}

	public void setTimeWindowType(int timeWindowType) {
		this.timeWindowType = timeWindowType;
	}

	/**
	 * 设置持久化数据扫描时间间隔，单位:毫秒
	 */
	public void setScanInterval(long scanInterval) {
		this.scanInterval = scanInterval;
	}

	public void setSegmentBoundSize(int segmentBoundSize) {
		this.segmentBoundSize = segmentBoundSize;
	}

	public int getSegmentBoundSize() {
		return segmentBoundSize;
	}
	public void setMetricsType(int metricsType) {
		if(metricsType != MetricsType_KeyTimeMetircs &&
				metricsType != MetricsType_TimeKeyMetircs &&
				metricsType != MetricsType_TimeMetircs &&
				metricsType != MetricsType_KeyMetircs
		)
			throw new MetricsException("错误的指标计算器类型："+metricsType + ",支持以下类型：MetricsType_KeyTimeMetircs = 0\n" +
					"\tMetricsType_TimeKeyMetircs = 1\n" +
					"\tMetricsType_TimeMetircs = 2\n" +
					"\tMetricsType_KeyMetircs = 3");
		this.metricsType = metricsType;
	}
	//	@Override
//	public void map(MapData data) {
//
//	}
//
//	@Override
//	public void persistent(Collection<KeyMetric> metrics) {
//
//	}
    @Override
	public void forceFlush(boolean cleanMetricsKey,boolean waitComplete) {
		baseMetrics.forceFlush( cleanMetricsKey,  waitComplete);
	}
	public void forceFlush() {
		forceFlush(false,false);
	}
	private int persistentDataHolderSize = 5000;

	public void setPersistentDataHolderSize(int persistentDataHolderSize) {
		this.persistentDataHolderSize = persistentDataHolderSize;
	}
	public void stopMetrics(){
        logger.info("Stop metrics begin......");
		baseMetrics.stopMetrics();
        logger.info("Stop metrics completed.");
	}

    private Object initLock = new Object();
	public  void init() {
		if(inited)
			return;
		synchronized (initLock){
			if(inited)
				return;
			builderMetrics();
            metricsLogAPI = buildMetricsLogAPI();
			if(metricsType == MetricsType_KeyTimeMetircs || metricsType == MetricsType_DEFAULT) {
				KeyTimeMetrics baseMetrics = new KeyTimeMetrics() {
					@Override
					public void map(MapData data) {
						Metrics.this.map(data);
					}

					@Override
					public void persistent(Collection<KeyMetric> metrics) {
						Metrics.this.persistent(metrics);
					}
				};
				baseMetrics.setTimeWindows(timeWindows);
				baseMetrics.setScanInterval(scanInterval);
				baseMetrics.setSegmentBoundSize(segmentBoundSize);
				baseMetrics.setPersistentDataHolderSize(persistentDataHolderSize);
				baseMetrics.setTimeWindowType(timeWindowType);
                baseMetrics.setMetricsLogAPI(metricsLogAPI);
				this.baseMetrics = baseMetrics;
			}
			else if(metricsType == MetricsType_TimeKeyMetircs) {
				TimeKeyMetrics baseMetrics = new TimeKeyMetrics() {
					@Override
					public void map(MapData data) {
						Metrics.this.map(data);
					}

					@Override
					public void persistent(Collection<KeyMetric> metrics) {
						Metrics.this.persistent(metrics);
					}
				};
				baseMetrics.setTimeWindows(timeWindows);
				baseMetrics.setScanInterval(scanInterval);
				baseMetrics.setSegmentBoundSize(segmentBoundSize);
				baseMetrics.setTimeWindowType(timeWindowType);
                baseMetrics.setMetricsLogAPI(metricsLogAPI);
				this.baseMetrics = baseMetrics;
			}
			else if(metricsType == MetricsType_TimeMetircs) {
				TimeMetrics baseMetrics = new TimeMetrics() {
					@Override
					public void map(MapData data) {
						Metrics.this.map(data);
					}

					@Override
					public void persistent(Collection<KeyMetric> metrics) {
						Metrics.this.persistent(metrics);
					}
				};
				baseMetrics.setTimeWindows(timeWindows);
				baseMetrics.setScanInterval(scanInterval);
				baseMetrics.setTimeWindowType(timeWindowType);
                baseMetrics.setMetricsLogAPI(metricsLogAPI);
				this.baseMetrics = baseMetrics;
			}
			else if(metricsType == MetricsType_KeyMetircs) {
				KeyMetrics baseMetrics = new KeyMetrics() {
					@Override
					public void map(MapData data) {
						Metrics.this.map(data);
					}

					@Override
					public void persistent(Collection<KeyMetric> metrics) {
						Metrics.this.persistent(metrics);
					}
				};
				baseMetrics.setSegmentBoundSize(segmentBoundSize);
                baseMetrics.setMetricsLogAPI(metricsLogAPI);
				this.baseMetrics = baseMetrics;

			}
			if(metricsName != null && !metricsName.equals("")){
				baseMetrics.setMetricsName( metricsName);
			}
			baseMetrics.init();
			inited = true;
		}
	}

	@Override
	public KeyMetric metric(String metricsKey, MapData data, KeyMetricBuilder metricBuilder) {
		return baseMetrics.metric(new MetricKey(metricsKey),data,metricBuilder);
	}

    @Override
    public KeyMetric metric(MetricKey metricsKey, MapData data, KeyMetricBuilder metricBuilder) {
        return baseMetrics.metric(metricsKey,data,metricBuilder);
    }
	public String getMetricsName() {
		return metricsName;
	}

	public void setMetricsName(String metricsName) {
		this.metricsName = metricsName;
	}


	@Override
	public BaseMetrics addMetricBuilder(MetricBuilder metricBuilder){
		if(metricBuilders == null){
			metricBuilders = new ArrayList<>();
		}
		metricBuilders.add(metricBuilder);
		return this;
	}
	@Override
	public List<MetricBuilder> getMetricBuilders(){
		return metricBuilders;
	}

	/**
	 * 对数据进行指标加工处理
	 * @param mapData
	 */
	@Override
	public void map(MapData mapData){
		List<MetricBuilder> metricBuilders = getMetricBuilders();
		if(metricBuilders != null && metricBuilders.size() > 0) {
			for (MetricBuilder metricBuilder : metricBuilders) {
				metric(metricBuilder.buildMetricKey(mapData), mapData, metricBuilder.innerMetricBuilder());
			}
		}
        else {
            throw new MetricsException("metricBuilders为空（没有正确设置和初始化Metrics），或者没有自定义实现map方法！");
        }

	}


}
