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

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/15 22:22
 * @author biaoping.yin
 * @version 1.0
 */
public class Kafka1InputDatatranPlugin extends KafkaInputDatatranPlugin {
	private Kafka1InputConfig kafkaInputConfig;
	private KafkaTranBatchConsumer2ndStore kafkaBatchConsumer2ndStore;

	public Kafka1InputDatatranPlugin(ImportContext importContext){
		super(  importContext);
		kafkaInputConfig = (Kafka1InputConfig) importContext.getInputConfig();
		this.jobType = "Kafka1InputDatatranPlugin";
	}

	@Override
	protected void initKafkaTranBatchConsumer2ndStore(BaseDataTran kafka2ESDataTran) throws Exception {
		final KafkaTranBatchConsumer2ndStore kafkaBatchConsumer2ndStore = new KafkaTranBatchConsumer2ndStore(kafka2ESDataTran,kafkaInputConfig);
		kafkaBatchConsumer2ndStore.setTopic(kafkaInputConfig.getKafkaTopic());
		kafkaBatchConsumer2ndStore.setBatchsize(importContext.getFetchSize());
		kafkaBatchConsumer2ndStore.setCheckinterval(kafkaInputConfig.getCheckinterval());
		kafkaBatchConsumer2ndStore.setConsumerPropes(kafkaInputConfig.getKafkaConfigs());
		kafkaBatchConsumer2ndStore.setPartitions(kafkaInputConfig.getConsumerThreads());
		kafkaBatchConsumer2ndStore.setDiscardRejectMessage(kafkaInputConfig.getDiscardRejectMessage());
//			kafkaBatchConsumer2ndStore.setPollTimeOut(kafkaContext.getPollTimeOut());
		kafkaBatchConsumer2ndStore.afterPropertiesSet();
		Thread consumerThread = new Thread(kafkaBatchConsumer2ndStore,"kafka-elasticsearch-BatchConsumer2ndStore");
		consumerThread.start();
	}

	@Override
	public void destroy(boolean waitTranStop) {
		if(kafkaBatchConsumer2ndStore != null){
			kafkaBatchConsumer2ndStore.shutdown();
		}
	}
}
