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

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.kafka.input.KafkaTranBatchConsumer2ndStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/15 22:22
 * @author biaoping.yin
 * @version 1.0
 */
public class Kafka2InputDatatranPlugin extends KafkaInputDatatranPlugin {
	private Kafka2InputConfig kafkaInputConfig;
	private static Logger logger = LoggerFactory.getLogger(Kafka2InputDatatranPlugin.class);
	private KafkaTranBatchConsumer2ndStore kafkaBatchConsumer2ndStore;
	private Thread consumerThread;
	public Kafka2InputDatatranPlugin(ImportContext importContext){
		super(  importContext);
		kafkaInputConfig = (Kafka2InputConfig) importContext.getInputConfig();
	}

	@Override
	protected void initKafkaTranBatchConsumer2ndStore(BaseDataTran kafka2ESDataTran) throws Exception {
		final KafkaTranBatchConsumer2ndStore kafkaBatchConsumer2ndStore = new KafkaTranBatchConsumer2ndStore(kafka2ESDataTran,kafkaInputConfig);
		kafkaBatchConsumer2ndStore.setTopic(kafkaInputConfig.getKafkaTopic());
		Properties config = kafkaInputConfig.getKafkaConfigs();
		boolean contain = config != null && !config.contains("max.poll.records");
		if(!contain)
			kafkaBatchConsumer2ndStore.setMaxPollRecords(importContext.getFetchSize());
		kafkaBatchConsumer2ndStore.setPollTimeout(kafkaInputConfig.getPollTimeOut());
		kafkaBatchConsumer2ndStore.setConsumerPropes(kafkaInputConfig.getKafkaConfigs());
		kafkaBatchConsumer2ndStore.setThreads(kafkaInputConfig.getConsumerThreads());
		kafkaBatchConsumer2ndStore.setDiscardRejectMessage(kafkaInputConfig.getDiscardRejectMessage());
		kafkaBatchConsumer2ndStore.setBatch(true);
		kafkaBatchConsumer2ndStore.setWorkThreads(kafkaInputConfig.getKafkaWorkThreads() == null?5:kafkaInputConfig.getKafkaWorkThreads());
		kafkaBatchConsumer2ndStore.setWorkQueue(kafkaInputConfig.getKafkaWorkQueue() == null?10:kafkaInputConfig.getKafkaWorkQueue());
//		kafkaBatchConsumer2ndStore.setPollTimeOut(kafkaInputConfig.getPollTimeOut());
		kafkaBatchConsumer2ndStore.afterPropertiesSet();
//		Thread consumerThread = new Thread(kafkaBatchConsumer2ndStore,"kafka-elasticsearch-BatchConsumer2ndStore");
//		consumerThread.start();
//		this.consumerThread = consumerThread;
		kafkaBatchConsumer2ndStore.run();
		this.kafkaBatchConsumer2ndStore = kafkaBatchConsumer2ndStore;
	}

	@Override
	public void destroy(boolean waitTranStop) {
		try {
			if (kafkaBatchConsumer2ndStore != null) {
				kafkaBatchConsumer2ndStore.shutdown();
			}
		}
		catch (Exception e){
			logger.warn("",e);
		}
		try {
			if(consumerThread != null){
				consumerThread.interrupt();
			}
		}
		catch (Exception e){
			logger.warn("",e);
		}

	}
}
