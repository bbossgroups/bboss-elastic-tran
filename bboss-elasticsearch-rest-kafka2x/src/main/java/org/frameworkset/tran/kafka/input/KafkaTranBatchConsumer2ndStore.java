package org.frameworkset.tran.kafka.input;
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


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.frameworkset.plugin.kafka.KafkaBatchConsumer2ndStore;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.es.output.AsynESOutPutDataTran;
import org.frameworkset.tran.kafka.KafkaContext;
import org.frameworkset.tran.kafka.KafkaMapRecord;
import org.frameworkset.tran.kafka.KafkaStringRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/28 10:41
 * @author biaoping.yin
 * @version 1.0
 */
public class KafkaTranBatchConsumer2ndStore extends KafkaBatchConsumer2ndStore {
	private KafkaContext kafkaContext;
	public KafkaTranBatchConsumer2ndStore(AsynESOutPutDataTran asynESOutPutDataTran, KafkaContext kafkaContext) {
		this.asynESOutPutDataTran = asynESOutPutDataTran;
		this.kafkaContext = kafkaContext;
	}

	private AsynESOutPutDataTran asynESOutPutDataTran;
	@Override
	public void store(ConsumerRecords<Object, Object> messages) throws Exception {

		List<Record> records = parserData(messages);
		asynESOutPutDataTran.appendInData(records);
	}

	@Override
	public void store(ConsumerRecord<Object, Object> message) throws Exception {

	}

	private void deserializeData(ConsumerRecord<Object,Object> consumerRecord,List<Record> results){
		Object value = consumerRecord.value();

		if (value instanceof List) {
			List rs = (List) value;

			for (int i = 0; i < rs.size(); i++) {
				Object v = rs.get(i);
				if (v instanceof Map) {
					results.add(new KafkaMapRecord(consumerRecord.key(), (Map<String, Object>) v));
				} else {
					results.add(new KafkaStringRecord(consumerRecord.key(), (String) v));
				}
			}
			//return new KafkaMapRecord((ConsumerRecord<Object, List<Map<String, Object>>>) data);
		} else if (value instanceof Map) {
			results.add( new KafkaMapRecord(consumerRecord.key(), (Map<String, Object>) value));
		} else if (value instanceof String) {
			results.add(new KafkaStringRecord(consumerRecord.key(), (String) value));
		}
		else{
			if(logger.isWarnEnabled()){
				logger.warn("unknown value type:{}",value.getClass().getName());
			}
			results.add(new KafkaStringRecord(consumerRecord.key(), String.valueOf( value)));
		}
//		throw new IllegalArgumentException(new StringBuilder().append("unknown consumerRecord").append(consumerRecord.toString()).toString());
	}
	protected List<Record> parserData(ConsumerRecords<Object, Object> messages) {
		List<Record> results = new ArrayList<Record>();
		for(ConsumerRecord consumerRecord:messages) {
//			ConsumerRecord consumerRecord = messages.get(k);
			deserializeData(consumerRecord,results);
			/**
			if(this.kafkaContext.getValueCodec() == null){
				handleData(consumerRecord,results);
			}
			else if (this.kafkaContext.getValueCodec() == KafkaImportConfig.CODEC_JSON) {
				Object value = consumerRecord.value();
				if (value instanceof List) {
					List<Map> rs = (List<Map>) value;

					for (int i = 0; i < rs.size(); i++) {
						results.add(new KafkaMapRecord(consumerRecord.key(), rs.get(i)));
					}

				} else {
					results.add( new KafkaMapRecord(consumerRecord.key(), (Map<String, Object>) value));
				}
			} else if (this.kafkaContext.getValueCodec() == KafkaImportConfig.CODEC_TEXT) {
				Object value = consumerRecord.value();
				if (value instanceof List) {
					List<String> rs = (List<String>) value;

					for (int i = 0; i < rs.size(); i++) {
						results.add(new KafkaStringRecord(consumerRecord.key(), rs.get(i)));
					}
					//return new KafkaMapRecord((ConsumerRecord<Object, List<Map<String, Object>>>) data);
				} else {
					results.add( new KafkaStringRecord(consumerRecord.key(), (String) value));
				}
			} else {
				Object value = consumerRecord.value();

				if (value instanceof List) {
					List rs = (List) value;

					for (int i = 0; i < rs.size(); i++) {
						Object v = rs.get(i);
						if (v instanceof Map) {
							results.add(new KafkaMapRecord(consumerRecord.key(), (Map<String, Object>) v));
						} else {
							results.add(new KafkaStringRecord(consumerRecord.key(), (String) v));
						}
					}
					//return new KafkaMapRecord((ConsumerRecord<Object, List<Map<String, Object>>>) data);
				} else if (value instanceof Map) {
					results.add( new KafkaMapRecord(consumerRecord.key(), (Map<String, Object>) value));
				} else if (value instanceof String) {
					results.add(new KafkaStringRecord(consumerRecord.key(), (String) value));
				}
				throw new IllegalArgumentException(new StringBuilder().append("unknown consumerRecord").append(consumerRecord.toString()).toString());
			}*/
		}
		return results;
	}



}
