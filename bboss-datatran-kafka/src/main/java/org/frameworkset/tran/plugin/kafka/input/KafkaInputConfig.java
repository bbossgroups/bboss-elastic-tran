package org.frameworkset.tran.plugin.kafka.input;
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

import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.kafka.codec.CodecUtil;
import org.frameworkset.tran.plugin.BaseConfig;

import java.util.Properties;

import static org.frameworkset.tran.metrics.job.MetricsConfig.DEFAULT_metricsInterval;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/20 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class KafkaInputConfig<T extends KafkaInputConfig> extends BaseConfig<T> implements InputConfig<T> {
	private Properties kafkaConfigs = null;
	private String kafkaTopic;
	private long checkinterval = 3000l;


	/**
	 * 默认十分钟执行一次拦截器监控数据afterCall方法
	 */
	private long metricsInterval = DEFAULT_metricsInterval;
	private int consumerThreads;
	private long pollTimeOut;
	public static final String CODEC_TEXT = "text";
	public static final String CODEC_TEXT_SPLIT = "text_split";
	public static final String CODEC_LONG = "long";
	public static final String CODEC_JSON = "json";
	public static final String CODEC_INTEGER = "int";
	public static final String CODEC_BYTE = "byte[]";
	/**
	 * json
	 * text
	 */
	private String valueCodec;

	/**
	 * json
	 * text
	 */
	private String keyCodec;
	/**
	 * 并行消费处理拒绝消息
	 */
	private String discardRejectMessage ;
	public T setKafkaConfigs(Properties kafkaConfigs) {
		this.kafkaConfigs = kafkaConfigs;
		return (T)this;
	}
	public T addKafkaConfig(String key,Object value){
		if(kafkaConfigs == null)
			kafkaConfigs = new Properties();
		kafkaConfigs.put(key,value);
		return (T)this;
	}
	public T setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
		return (T)this;
	}

	public Properties getKafkaConfigs(){
		return kafkaConfigs;
	}

	public String getKafkaTopic(){
		return kafkaTopic;
	}

	public long getCheckinterval() {
		return checkinterval;
	}

	public T setCheckinterval(long checkinterval) {
		this.checkinterval = checkinterval;
		return (T)this;
	}

	public String getDiscardRejectMessage() {
		return discardRejectMessage;
	}

	public T setDiscardRejectMessage(String discardRejectMessage) {
		this.discardRejectMessage = discardRejectMessage;
		return (T)this;
	}

	public int getConsumerThreads() {
		return consumerThreads;
	}

	public T setConsumerThreads(int threads) {
		this.consumerThreads = threads;
		return (T)this;
	}

	public long getPollTimeOut() {
		return pollTimeOut;
	}

	public T setPollTimeOut(long pollTimeOut) {
		this.pollTimeOut = pollTimeOut;
		return (T)this;
	}

	public String getValueCodec() {
		return valueCodec;
	}

	public T setValueCodec(String valueCodec) {
		this.valueCodec = valueCodec;
		return (T)this;
	}

	public String getKeyCodec() {
		return keyCodec;
	}

	public T setKeyCodec(String keyCodec) {
		this.keyCodec = keyCodec;
		return (T)this;
	}
	private Integer kafkaWorkThreads;
	private Integer kafkaWorkQueue;
	public Integer getKafkaWorkThreads(){
		return kafkaWorkThreads;
	}
	public Integer getKafkaWorkQueue(){
		return kafkaWorkQueue;
	}

	public T setKafkaWorkThreads(Integer kafkaWorkThreads) {
		this.kafkaWorkThreads = kafkaWorkThreads;
		return (T)this;
	}

	public T setKafkaWorkQueue(Integer kafkaWorkQueue) {
		this.kafkaWorkQueue = kafkaWorkQueue;
		return (T)this;
	}

	private void preHandlerCodec(){
		Properties properties = this.getKafkaConfigs();
		if(!properties.containsKey("value.deserializer")){

			if(this.getValueCodec() != null) {
				properties.put("value.deserializer", CodecUtil.getDeserializer(getValueCodec()));
				if(CODEC_TEXT_SPLIT.equals(getValueCodec())){
					properties.put("ext.value.deserializer.splitChar",getFieldSplit());
					properties.put("ext.value.deserializer.cellMappingList",getCellMappingList());
				}
			}
			else{
				properties.put("value.deserializer", CodecUtil.getDeserializer(CODEC_JSON));
			}

		}
		if(!properties.containsKey("key.deserializer") ){
			if(this.getKeyCodec() != null) {
				properties.put("key.deserializer", CodecUtil.getDeserializer(getKeyCodec()));
			}
			else{
				properties.put("key.deserializer", CodecUtil.getDeserializer(CODEC_TEXT));
			}

		}

	}

	@Override
	public void build(ImportContext importContext,ImportBuilder importBuilder) {
		preHandlerCodec();
	}
	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext){
		DataTranPlugin dataTranPlugin = new KafkaDataTranPluginImpl(importContext);
		return dataTranPlugin;
	}
	public long getMetricsInterval() {
		return metricsInterval;
	}

	public T setMetricsInterval(long metricsInterval) {
		this.metricsInterval = metricsInterval;
		return (T)this;
	}

}
