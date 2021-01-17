package org.frameworkset.tran.kafka;
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

import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.DefualtExportResultHandler;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.config.BaseImportBuilder;
import org.frameworkset.tran.kafka.codec.CodecUtil;

import java.util.Properties;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class KafkaExportBuilder extends BaseImportBuilder {
	private Properties kafkaConfigs = new Properties();
	private String kafkaTopic;
	private long checkinterval = 3000l;
	private int consumerThreads;
	private int pollTimeOut;





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
	private String discardRejectMessage  ;


	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler(exportResultHandler);
	}

	public Properties getKafkaConfigs() {
		return kafkaConfigs;
	}

	public KafkaExportBuilder addKafkaConfig(String key, String value){
		kafkaConfigs.put(key,value);
		return this;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public KafkaExportBuilder setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
		return this;
	}

	public long getCheckinterval() {
		return checkinterval;
	}

	public KafkaExportBuilder setCheckinterval(long checkinterval) {
		this.checkinterval = checkinterval;
		return this;
	}



	public long getPollTimeOut() {
		return pollTimeOut;
	}

	public KafkaExportBuilder setPollTimeOut(int pollTimeOut) {
		this.pollTimeOut = pollTimeOut;
		return this;
	}

	public String getDiscardRejectMessage() {
		return discardRejectMessage;
	}

	public KafkaExportBuilder setDiscardRejectMessage(String discardRejectMessage) {
		this.discardRejectMessage = discardRejectMessage;
		return this;
	}

	public int getConsumerThreads() {
		return consumerThreads;
	}

	public KafkaExportBuilder setConsumerThreads(int consumerThreads) {
		this.consumerThreads = consumerThreads;
		return this;
	}

	@Override
	public DataStream builder() {
		super.builderConfig();
//		this.buildDBConfig();
//		this.buildStatusDBConfig();
		try {
			if(logger.isInfoEnabled()) {
				logger.info("Kafka Import Configs:");
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}
		KafkaImportConfig es2DBImportConfig = new KafkaImportConfig();
		super.buildImportConfig(es2DBImportConfig);
		es2DBImportConfig.setCheckinterval(this.getCheckinterval());
		es2DBImportConfig.setDiscardRejectMessage(this.getDiscardRejectMessage());
		es2DBImportConfig.setValueCodec(this.getValueCodec());
		es2DBImportConfig.setKeyCodec(this.getKeyCodec());
		preHandlerCodec();
		es2DBImportConfig.setKafkaConfigs(this.getKafkaConfigs());
		es2DBImportConfig.setKafkaTopic(this.getKafkaTopic());
		es2DBImportConfig.setPollTimeOut(this.getPollTimeOut());
		es2DBImportConfig.setConsumerThreads(this.getConsumerThreads());
		es2DBImportConfig.setValueCodec(this.getValueCodec());
		es2DBImportConfig.setKafkaWorkQueue(kafkaWorkQueue);
		es2DBImportConfig.setKafkaWorkThreads(kafkaWorkThreads);
		DataStream dataStream = createDataStream();//new Kafka2ESDataStreamImpl();
		dataStream.setImportConfig(es2DBImportConfig);
		dataStream.setImportContext(this.buildImportContext(es2DBImportConfig));

		return dataStream;
	}
	private Integer kafkaWorkThreads;
	private Integer kafkaWorkQueue;



	public Integer getKafkaWorkThreads(){
		return kafkaWorkThreads;
	}
	public Integer getKafkaWorkQueue(){
		return kafkaWorkQueue;
	}

	public KafkaExportBuilder setKafkaWorkThreads(Integer kafkaWorkThreads) {
		this.kafkaWorkThreads = kafkaWorkThreads;
		return this;
	}

	public KafkaExportBuilder setKafkaWorkQueue(Integer kafkaWorkQueue) {
		this.kafkaWorkQueue = kafkaWorkQueue;
		return this;
	}
	private void preHandlerCodec(){
		Properties properties = this.getKafkaConfigs();
		if(!properties.containsKey("value.deserializer")){

			if(this.getValueCodec() != null) {
				properties.put("value.deserializer", CodecUtil.getDeserializer(getValueCodec()));
			}
			else{
				properties.put("value.deserializer", CodecUtil.getDeserializer(KafkaImportConfig.CODEC_JSON));
			}

		}
		if(!properties.containsKey("key.deserializer") ){
			if(this.getKeyCodec() != null) {
				properties.put("key.deserializer", CodecUtil.getDeserializer(getKeyCodec()));
			}
			else{
				properties.put("key.deserializer", CodecUtil.getDeserializer(KafkaImportConfig.CODEC_TEXT));
			}

		}

	}

	public String getValueCodec() {
		return valueCodec;
	}

	public KafkaExportBuilder setValueCodec(String valueCodec) {
		this.valueCodec = valueCodec;
		return this;
	}

	public String getKeyCodec() {
		return keyCodec;
	}

	public KafkaExportBuilder setKeyCodec(String keyCodec) {
		this.keyCodec = keyCodec;
		return this;
	}
}
