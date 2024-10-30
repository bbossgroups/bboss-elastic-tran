package org.frameworkset.tran.plugin.metrics.output;
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.OutputPlugin;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: 指标统计插件</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:48
 * @author biaoping.yin
 * @version 1.0
 */
public class MetricsOutputConfig extends BaseConfig implements OutputConfig {

	private List<ETLMetrics> metrics;


	/**
	 * 指标时间维度字段，不是设置默认采用当前时间，否则采用字段对应的时间值
	 */
	private String dataTimeField;
    private Integer timeWindowType;
	private boolean useDefaultMapData = false;

	public MetricsOutputConfig addMetrics(ETLMetrics metrics){
		if(this.metrics == null){
			this.metrics = new ArrayList<>();
		}
		this.metrics.add(metrics);
		return this;
	}

	public List<ETLMetrics> getMetrics() {
		return metrics;
	}

	@Override
	public void build(ImportContext importContext,ImportBuilder importBuilder) {
		if(metrics == null || metrics.size() == 0){
			throw new DataImportException("未正确设置metrics,可以通过addMetrics方法添加注册ETLMetrics!，参考文档：\r\nhttps://esdoc.bbossgroups.com/#/etl-metrics");
		}
//      迁移至DataTranPluginImpl.init---》importContext.initETLMetrics()
//		for(Metrics metrics: this.metrics){
//			metrics.init();
//		}
	}

	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new MetricsOutputDataTranPlugin(importContext);
	}

	public boolean isUseDefaultMapData() {
		return useDefaultMapData;
	}

    /**
     * 控制ETLMetrics是否构建默认MapData对象
     * true 如果ETLMetrics没有设置dataTimeField，没有提供自定义的MapdataBuilder，则使用默认构建MapData对象
     * false 不构建默认MapData对象
     * @param useDefaultMapData
     * @return
     */
	public MetricsOutputConfig setUseDefaultMapData(boolean useDefaultMapData) {
		this.useDefaultMapData = useDefaultMapData;
		return this;
	}

	public String getDataTimeField() {
		return dataTimeField;
	}

	/**
	 * 设置指标时间维度字段，不是设置默认采用当前时间，否则采用字段对应的时间值
	 * @param dataTimeField
	 * @return
	 */
	public MetricsOutputConfig setDataTimeField(String dataTimeField) {
		this.dataTimeField = dataTimeField;
		return this;
	}
    public Integer getTimeWindowType() {
        return timeWindowType;
    }

    public MetricsOutputConfig setTimeWindowType(Integer timeWindowType) {
        this.timeWindowType = timeWindowType;
        return this;
    }
}
