package org.frameworkset.tran.metrics;
/**
 * Copyright 2008 biaoping.yin
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

import org.frameworkset.tran.BaseExportResultHandler;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.metrics.entity.KeyMetric;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.job.KeyMetricBuilder;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.metrics.job.MetricsConfig;
import org.frameworkset.tran.metrics.job.builder.MetricBuilder;
import org.frameworkset.tran.task.TaskCommand;

import java.text.DateFormat;
import java.util.Collection;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 10:20
 * @author biaoping.yin
 * @version 1.0
 */
public class TimeWindowExportResultHandler<DATA,RESULT> extends BaseExportResultHandler<DATA,RESULT> {

	private Metrics keyMetrics ;



	private String metricKeyPrex;
	public TimeWindowExportResultHandler(final String metricKeyPrex, ExportResultHandler exportResultHandler, OutputConfig outputConfig){
		super(exportResultHandler);
		this.metricKeyPrex = metricKeyPrex;
		//指标key不会长期有效，所以不宜用其他指标类型，适合使用MetricsType_TimeMetircs
		keyMetrics = new Metrics(Metrics.MetricsType_TimeMetircs) {
			@Override
			public void builderMetrics(){
				addMetricBuilder(new MetricBuilder() {
					@Override
					public String buildMetricKey(MapData mapData){
						TaskMetrics taskMetrics = (TaskMetrics) mapData.getData();

						StringBuilder mkey = new StringBuilder();
						mkey.append(metricKeyPrex).append("-").append(taskMetrics.getJobNo());
						return mkey.toString();
					}
					@Override
					public KeyMetricBuilder metricBuilder(){
						return () -> new TimeTaskMetric(TimeWindowExportResultHandler.this);
					}
				});
				// key metrics中包含两个segment(S0,S1)
				setSegmentBoundSize(5000000);
				setTimeWindows(outputConfig.getMetricsAggWindow() );
			}
			@Override
			public void persistent(Collection< KeyMetric> metrics) {
				metrics.forEach(keyMetric->{
					TimeTaskMetric timeTaskMetric = (TimeTaskMetric)keyMetric;
					MetricsTaskcommand metricsTaskcommand = new MetricsTaskcommand();
					metricsTaskcommand.setImportContext(timeTaskMetric.getImportContext());
					metricsTaskcommand.setJobContext(timeTaskMetric.getJobContext());
					TaskMetrics taskMetrics = new TaskMetrics();
					taskMetrics.setJobName(timeTaskMetric.getJobName());
					taskMetrics.setJobId(timeTaskMetric.getJobId());
					taskMetrics.setJobNo(timeTaskMetric.getJobNo());
					taskMetrics.setLastValue(timeTaskMetric.getLastValue());
					taskMetrics.setJobStartTime(timeTaskMetric.getJobStartTime());
					taskMetrics.setTaskStartTime(timeTaskMetric.getTaskStartTime());
					taskMetrics.setTaskEndTime(timeTaskMetric.getTaskEndTime());
					taskMetrics.setTotalRecords(timeTaskMetric.getTotalRecords());

					taskMetrics.setTotalFailedRecords(timeTaskMetric.getTotalFailedRecords());
					taskMetrics.setTotalIgnoreRecords(timeTaskMetric.getTotalIgnoreRecords());
					taskMetrics.setTotalSuccessRecords(timeTaskMetric.getTotalSuccessRecords());
					taskMetrics.setSuccessRecords(timeTaskMetric.getSuccessRecords());
					taskMetrics.setFailedRecords(timeTaskMetric.getFailedRecords());
					taskMetrics.setIgnoreRecords(timeTaskMetric.getIgnoreRecords());
					taskMetrics.setRecords(timeTaskMetric.getRecords());
					taskMetrics.setTaskNo(timeTaskMetric.getTaskNo());
					metricsTaskcommand.setTaskMetrics(taskMetrics);
					exportResultHandler.success(metricsTaskcommand,timeTaskMetric.getResult());
				});

			}
		};



		keyMetrics.init();




	}

	@Override
	public void destroy(){
		//强制刷指标数据
		if(keyMetrics != null) {
//			keyMetrics.forceFlush(true);
			keyMetrics.stopMetrics();
		}

	}

	@Override
	public void success(TaskCommand<DATA, RESULT> taskCommand, RESULT result) {
//		super.success(taskCommand, result);、
		TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
		MetricsMapData mapData = new MetricsMapData();
		mapData.setDataTime(new Date());
		DateFormat dayFormat = MetricsConfig.getDayFormat();
		DateFormat hourFormat = MetricsConfig.getHourFormat();
		DateFormat minuteFormat = MetricsConfig.getMinuteFormat();
		mapData.setDayFormat(dayFormat);
		mapData.setHourFormat(hourFormat);
		mapData.setMinuteFormat(minuteFormat);
		mapData.setResult(result);
		mapData.setData(taskMetrics);
		mapData.setJobContext(taskCommand.getJobContext());
		mapData.setImportContext(taskCommand.getImportContext());
		keyMetrics.map(mapData);

	}
	public String getMetricKeyPrex() {
		return metricKeyPrex;
	}

}
