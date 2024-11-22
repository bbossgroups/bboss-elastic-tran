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


import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.frameworkset.plugin.kafka.KafkaBatchConsumer2ndStore;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.kafka.KafkaData;
import org.frameworkset.tran.kafka.KafkaMapRecord;
import org.frameworkset.tran.kafka.KafkaStringRecord;
import org.frameworkset.tran.kafka.codec.CodecObjectUtil;
import org.frameworkset.tran.plugin.kafka.input.Kafka1InputConfig;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.frameworkset.tran.plugin.kafka.input.KafkaInputConfig.CODEC_JSON;
import static org.frameworkset.tran.plugin.kafka.input.KafkaInputConfig.CODEC_TEXT;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/28 10:41
 * @author biaoping.yin
 * @version 1.0
 */
public class KafkaTranBatchConsumer2ndStore extends KafkaBatchConsumer2ndStore {
	private Kafka1InputConfig kafka1InputConfig;
	private Deserializer valueDeserializer;
	private Deserializer keyDeserializer;

	public KafkaTranBatchConsumer2ndStore(BaseDataTran asynESOutPutDataTran, Kafka1InputConfig kafka1InputConfig) {
		this.asynESOutPutDataTran = asynESOutPutDataTran;
		this.kafka1InputConfig = kafka1InputConfig;
		if(kafka1InputConfig.getValueCodec() != null) {
			valueDeserializer = CodecObjectUtil.getDeserializer(kafka1InputConfig.getValueCodec());
		}
		else{
			valueDeserializer = CodecObjectUtil.getDeserializer(CODEC_JSON);
		}
		if(kafka1InputConfig.getKeyCodec() != null) {
			keyDeserializer = CodecObjectUtil.getDeserializer(kafka1InputConfig.getKeyCodec());
		}
		else{
			keyDeserializer = CodecObjectUtil.getDeserializer(CODEC_TEXT);
		}
	}

	private BaseDataTran asynESOutPutDataTran;
	@Override
	public void store(List<MessageAndMetadata<byte[], byte[]>> messages) throws Exception {

		List<Record> records = parserData(messages);
		asynESOutPutDataTran.appendData(new KafkaData(records));
	}
    private Map<String,Object> buildMetas(MessageAndMetadata<byte[], byte[]> consumerRecord,Object key){
        Map<String,Object> metas = new LinkedHashMap<>();
        metas.put("topic",consumerRecord.topic());
        metas.put("offset",consumerRecord.offset());
        metas.put("key",key);
        metas.put("partition",consumerRecord.partition());
        metas.put("timestamp",consumerRecord.timestamp());
        return metas;
    }
	@Override
	public void store(MessageAndMetadata<byte[], byte[]> message) throws Exception {
		List<MessageAndMetadata<byte[], byte[]>> messages = new ArrayList<MessageAndMetadata<byte[], byte[]>>();
		messages.add(message);
		store(messages);
	}
	private void deserializeData(MessageAndMetadata<byte[], byte[]> consumerRecord, List<Record> results){
		Object value = valueDeserializer.deserialize(consumerRecord.topic(),consumerRecord.message());
		Object key = keyDeserializer.deserialize(consumerRecord.topic(),consumerRecord.key());
        Map<String,Object> metas = buildMetas( consumerRecord,key);
		if (value instanceof List) {
			List rs = (List) value;

			for (int i = 0; i < rs.size(); i++) {
				Object v = rs.get(i);
				if (v instanceof Map) {
					results.add(new KafkaMapRecord(asynESOutPutDataTran.getTaskContext(), asynESOutPutDataTran.getImportContext(), key, (Map<String, Object>) v,consumerRecord.offset(),metas));
				} else {
					results.add(new KafkaStringRecord(asynESOutPutDataTran.getTaskContext(),asynESOutPutDataTran.getImportContext(),key, (String) v,consumerRecord.offset(),metas));
				}
			}
			//return new KafkaMapRecord((ConsumerRecord<Object, List<Map<String, Object>>>) data);
		} else if (value instanceof Map) {
			results.add( new KafkaMapRecord(asynESOutPutDataTran.getTaskContext(),asynESOutPutDataTran.getImportContext(),key, (Map<String, Object>) value,consumerRecord.offset(),metas));
		} else if (value instanceof String) {
			results.add(new KafkaStringRecord(asynESOutPutDataTran.getTaskContext(),asynESOutPutDataTran.getImportContext(),key, (String) value,consumerRecord.offset(),metas));
		}
		else{
			if(logger.isWarnEnabled()){
				logger.warn("unknown value type:{}",value.getClass().getName());
			}
			results.add(new KafkaStringRecord(asynESOutPutDataTran.getTaskContext(),asynESOutPutDataTran.getImportContext(),key, String.valueOf( value),consumerRecord.offset(),metas));
		}
//		throw new IllegalArgumentException(new StringBuilder().append("unknown consumerRecord").append(consumerRecord.toString()).toString());
	}
	protected List<Record> parserData(List<MessageAndMetadata<byte[], byte[]>> messages) {
		List<Record> results = new ArrayList<Record>();
		for(int k = 0; k < messages.size(); k ++) {
			MessageAndMetadata<byte[], byte[]> consumerRecord = messages.get(k);
			deserializeData(  consumerRecord,  results);
			/**
			if (this.kafka1InputConfig.getValueCodec() == KafkaImportConfig.CODEC_JSON) {
				Object value = valueDeserializer.deserialize(consumerRecord.topic(),consumerRecord.message());
				if (value instanceof List) {
					List<Map> rs = (List<Map>) value;

					for (int i = 0; i < rs.size(); i++) {
						results.add(new KafkaMapRecord(consumerRecord.key(), rs.get(i)));
					}

				} else {
					results.add( new KafkaMapRecord(consumerRecord.key(), (Map<String, Object>) value));
				}
			} else if (this.kafka1InputConfig.getValueCodec() == KafkaImportConfig.CODEC_TEXT) {
				Object value = valueDeserializer.deserialize(consumerRecord.topic(),consumerRecord.message());
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
				Object value = valueDeserializer.deserialize(consumerRecord.topic(),consumerRecord.message());

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
				throw new IllegalArgumentException(new StringBuilder().append("unknown consumerRecord with codec[").append(this.kafka1InputConfig.getValueCodec()).append("]").append(consumerRecord.toString()).toString());
			}*/
		}
		return results;
	}

	@Override
	public void closeService() {
		if(valueDeserializer != null){
			valueDeserializer.close();
		}
		if(keyDeserializer != null){
			keyDeserializer.close();
		}
	}

}
