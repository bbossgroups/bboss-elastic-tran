package org.frameworkset.tran.plugin.kafka.output;
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.kafka.output.KafkaSend;
import org.frameworkset.tran.metrics.TimeWindowExportResultHandler;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.JsonRecordGenerator;
import org.frameworkset.tran.util.RecordGenerator;

import java.io.Writer;
import java.util.Map;
import java.util.Properties;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/23 11:42
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class KafkaOutputConfig extends BaseConfig implements OutputConfig {
	private Properties kafkaConfigs = null;
	public static final String metricKey_kafkaoutputplutin = "kafkaoutputplutin";
	/**
	 * 是否启用success监控指标按照聚合统计后的指标输出，默认false
	 * true 启用
	 * false 不启用
	 */
	private boolean enableMetricsAgg;
	/**
	 * 聚合统计时间窗口，默认1分钟
	 */
	private int metricsAggWindow = 60;

	private KafkaSend kafkaSend;
//	private long logsendTaskMetric = 10000l;

	public boolean isKafkaAsynSend() {
		return kafkaAsynSend;
	}

	public KafkaOutputConfig setKafkaSend(KafkaSend kafkaSend) {
		this.kafkaSend = kafkaSend;
		return this;
	}

	public KafkaSend getKafkaSend() {
		return kafkaSend;
	}
	public KafkaOutputConfig setKafkaAsynSend(boolean kafkaAsynSend) {
		this.kafkaAsynSend = kafkaAsynSend;
		return this;
	}

	private boolean kafkaAsynSend = true;
	public RecordGenerator getRecordGenerator() {
		return recordGenerator;
	}

	public KafkaOutputConfig setRecordGenerator(RecordGenerator recordGenerator) {
		this.recordGenerator = recordGenerator;
		return this;
	}
	public void generateReocord(TaskContext context, CommonRecord record, Writer builder) throws Exception{
		if(builder == null){
			builder = RecordGenerator.tranDummyWriter;
		}
		getRecordGenerator().buildRecord(context,record,builder);
	}
	/**
	 * 输出文件记录处理器:org.frameworkset.tran.util.ReocordGenerator
	 */
	private RecordGenerator recordGenerator;
	public String getTopic() {
		return topic;
	}

	public KafkaOutputConfig setTopic(String topic) {
		this.topic = topic;
		return this;
	}

	private String topic;
	public KafkaOutputConfig addKafkaProperty(String name,String value){
		if(kafkaConfigs == null)
			kafkaConfigs = new Properties();
		kafkaConfigs.setProperty(name,value);
		return this;
	}
	public KafkaOutputConfig addKafkaProperties(Map properties){
		if(properties == null || properties.size() == 0)
			return this;
		if(kafkaConfigs == null)
			kafkaConfigs = new Properties();
		kafkaConfigs.putAll(properties);
		return this;
	}
	public Properties getKafkaConfigs() {
		return kafkaConfigs;
	}

//	public long getLogsendTaskMetric() {
//		return logsendTaskMetric;
//	}
//
//	public KafkaOutputConfig setLogsendTaskMetric(long logsendTaskMetric) {
//		this.logsendTaskMetric = logsendTaskMetric;
//		return this;
//	}

	@Override
	public void build(ImportBuilder importBuilder) {
		if(recordGenerator == null){
			recordGenerator = new JsonRecordGenerator();
		}
	}

	public WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		if(enableMetricsAgg) {
			TimeWindowExportResultHandler timeWindowExportResultHandler = new TimeWindowExportResultHandler(metricKey_kafkaoutputplutin,
					exportResultHandler, this);
			return timeWindowExportResultHandler;
		}
		else{
			return super.buildExportResultHandler(exportResultHandler);
		}
	}


	public boolean isEnableMetricsAgg() {
		return enableMetricsAgg;
	}

	public KafkaOutputConfig setEnableMetricsAgg(boolean enableMetricsAgg) {
		this.enableMetricsAgg = enableMetricsAgg;
		return this;
	}

	@Override
	public int getMetricsAggWindow() {
		return metricsAggWindow;
	}

	public KafkaOutputConfig setMetricsAggWindow(int metricsAggWindow) {
		this.metricsAggWindow = metricsAggWindow;
		return this;
	}
}
