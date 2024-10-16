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
import org.frameworkset.tran.metrics.job.builder.MetricBuilder;

import java.util.Collection;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/8/21
 * @author biaoping.yin
 * @version 1.0
 */
public interface BaseMetrics {


	default public BaseMetrics addMetricBuilder(MetricBuilder metricBuilder){
		return this;
	}
    default public void setMetricsLogAPI(MetricsLogAPI metricsLogAPI){
        
    }

    default public MetricsLogAPI getMetricsLogAPI(){
        return null;
    }
	default public List<MetricBuilder> getMetricBuilders(){
		return null;
	}

	default public void builderMetrics(){

	}

	public void stopMetrics();
	/**
	 * 对数据进行指标加工处理
	 * @param mapData
	 */
	public void map(MapData mapData);

	/**
	 * 定时存储指标表数据
	 * @param metrics
	 */
	public abstract void persistent(Collection<KeyMetric> metrics);

	public KeyMetric metric(String metricsKey, MapData data, KeyMetricBuilder metricBuilder);


	/**
	 *
	 * @param cleanMetricsKey 参数含义说明：
	 *                           TimeMetrics和KeyTimeMetrics true 代表清理指标key，false代表不清理指标key
	 *                           TimeKeyMetrics和KeyMetrics   true 代表初始化指标容器，false代表清理指标容器为null（意味后续不再使用对应的指标容器）
     * @param waitComplete 是否等待任务完成
	 */
	public void forceFlush(boolean cleanMetricsKey,boolean waitComplete);
	public void init();
	public void setMetricsName(String metricsName);

}
