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

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/20 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class KafkaInputConfig extends BaseConfig implements InputConfig {
	private Properties kafkaConfigs = null;
	private String kafkaTopic;
	private long checkinterval = 3000l;
	private int consumerThreads;
	private long pollTimeOut;
	public static final String CODEC_TEXT = "text";
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
	public void setKafkaConfigs(Properties kafkaConfigs) {
		this.kafkaConfigs = kafkaConfigs;
	}

	public void setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
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

	public void setCheckinterval(long checkinterval) {
		this.checkinterval = checkinterval;
	}

	public String getDiscardRejectMessage() {
		return discardRejectMessage;
	}

	public void setDiscardRejectMessage(String discardRejectMessage) {
		this.discardRejectMessage = discardRejectMessage;
	}

	public int getConsumerThreads() {
		return consumerThreads;
	}

	public void setConsumerThreads(int threads) {
		this.consumerThreads = threads;
	}

	public long getPollTimeOut() {
		return pollTimeOut;
	}

	public void setPollTimeOut(long pollTimeOut) {
		this.pollTimeOut = pollTimeOut;
	}

	public String getValueCodec() {
		return valueCodec;
	}

	public void setValueCodec(String valueCodec) {
		this.valueCodec = valueCodec;
	}

	public String getKeyCodec() {
		return keyCodec;
	}

	public void setKeyCodec(String keyCodec) {
		this.keyCodec = keyCodec;
	}
	private Integer kafkaWorkThreads;
	private Integer kafkaWorkQueue;
	public Integer getKafkaWorkThreads(){
		return kafkaWorkThreads;
	}
	public Integer getKafkaWorkQueue(){
		return kafkaWorkQueue;
	}

	public void setKafkaWorkThreads(Integer kafkaWorkThreads) {
		this.kafkaWorkThreads = kafkaWorkThreads;
	}

	public void setKafkaWorkQueue(Integer kafkaWorkQueue) {
		this.kafkaWorkQueue = kafkaWorkQueue;
	}

	private void preHandlerCodec(){
		Properties properties = this.getKafkaConfigs();
		if(!properties.containsKey("value.deserializer")){

			if(this.getValueCodec() != null) {
				properties.put("value.deserializer", CodecUtil.getDeserializer(getValueCodec()));
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
	public void build(ImportBuilder importBuilder) {
		preHandlerCodec();
	}
	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext){
		DataTranPlugin dataTranPlugin = new KafkaDataTranPluginImpl(importContext);
		return dataTranPlugin;
	}


}
