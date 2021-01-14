package org.frameworkset.tran.kafka.input.es;
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.kafka.input.BaseKafkaInputPlugin;
import org.frameworkset.tran.kafka.input.KafkaTranBatchConsumer2ndStore;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/15 22:22
 * @author biaoping.yin
 * @version 1.0
 */
public class Kafka2ESInputPlugin extends BaseKafkaInputPlugin {
	public Kafka2ESInputPlugin(ImportContext importContext) {
		super(importContext);
	}
	@Override
	public void beforeInit() {
		this.initES(importContext.getApplicationPropertiesFile());
		initOtherDSes(importContext.getConfigs());
		super.beforeInit();
	}



	@Override
	protected void initKafkaTranBatchConsumer2ndStore(Kafka2ESDataTran kafka2ESDataTran) throws Exception {
		final KafkaTranBatchConsumer2ndStore kafkaBatchConsumer2ndStore = new KafkaTranBatchConsumer2ndStore(kafka2ESDataTran,kafkaContext);
		kafkaBatchConsumer2ndStore.setTopic(kafkaContext.getKafkaTopic());
		kafkaBatchConsumer2ndStore.setBatchsize(importContext.getFetchSize());
		kafkaBatchConsumer2ndStore.setCheckinterval(kafkaContext.getCheckinterval());
		kafkaBatchConsumer2ndStore.setConsumerPropes(kafkaContext.getKafkaConfigs());
		kafkaBatchConsumer2ndStore.setPartitions(kafkaContext.getConsumerThreads());
		kafkaBatchConsumer2ndStore.setDiscardRejectMessage(kafkaContext.getDiscardRejectMessage());
//			kafkaBatchConsumer2ndStore.setPollTimeOut(kafkaContext.getPollTimeOut());
		kafkaBatchConsumer2ndStore.afterPropertiesSet();
		Thread consumerThread = new Thread(kafkaBatchConsumer2ndStore,"kafka-elasticsearch-BatchConsumer2ndStore");
		consumerThread.start();
	}
}
