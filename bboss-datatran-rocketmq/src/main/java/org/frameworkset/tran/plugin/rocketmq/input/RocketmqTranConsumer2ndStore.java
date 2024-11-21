package org.frameworkset.tran.plugin.rocketmq.input;
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


import org.frameworkset.rocketmq.RocketmqConsumer2ndStore;
import org.frameworkset.rocketmq.codec.RocketmqMessage;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.plugin.rocketmq.input.RocketmqInputConfig;
import org.frameworkset.tran.rocketmq.RocketmqData;
import org.frameworkset.tran.rocketmq.RocketmqMapRecord;
import org.frameworkset.tran.rocketmq.RocketmqStringRecord;

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
public class RocketmqTranConsumer2ndStore extends RocketmqConsumer2ndStore<Object> {
	private RocketmqInputConfig rocketmqInputConfig;
	public RocketmqTranConsumer2ndStore(BaseDataTran asynESOutPutDataTran, RocketmqInputConfig rocketmqInputConfig) {
		this.asynESOutPutDataTran = asynESOutPutDataTran;
		this.rocketmqInputConfig = rocketmqInputConfig;
	}

	private BaseDataTran asynESOutPutDataTran;
	@Override
	public void store(List<RocketmqMessage<Object>> messages) throws Exception {

		List<Record> records = parserData(messages);
		asynESOutPutDataTran.appendData(new RocketmqData(records));
	}
 
	private void deserializeData(RocketmqMessage<Object> consumerRecord,List<Record> results){
		Object value = consumerRecord.getData();
        String key = consumerRecord.getMessageExt().getKeys();
        long offset = consumerRecord.getMessageExt().getQueueOffset();
		if (value instanceof List) {
			List rs = (List) value;

			for (int i = 0; i < rs.size(); i++) {
				Object v = rs.get(i);
				if (v instanceof Map) {
					results.add(new RocketmqMapRecord(asynESOutPutDataTran.getTaskContext(),asynESOutPutDataTran.getImportContext(),key, (Map<String, Object>) v,offset));
				} else {
					results.add(new RocketmqStringRecord(asynESOutPutDataTran.getTaskContext(),asynESOutPutDataTran.getImportContext(),key, (String) v,offset));
				}
			}
			//return new RocketmqMapRecord((ConsumerRecord<Object, List<Map<String, Object>>>) data);
		} else if (value instanceof Map) {
			results.add( new RocketmqMapRecord(asynESOutPutDataTran.getTaskContext(),asynESOutPutDataTran.getImportContext(),key, (Map<String, Object>) value,offset));
		} else if (value instanceof String) {
			results.add(new RocketmqStringRecord(asynESOutPutDataTran.getTaskContext(),asynESOutPutDataTran.getImportContext(),key, (String) value,offset));
		}
		else{
			if(logger.isWarnEnabled()){
				logger.warn("unknown value type:{}",value.getClass().getName());
			}
			results.add(new RocketmqStringRecord(asynESOutPutDataTran.getTaskContext(),asynESOutPutDataTran.getImportContext(),key, String.valueOf( value),offset));
		}
//		throw new IllegalArgumentException(new StringBuilder().append("unknown consumerRecord").append(consumerRecord.toString()).toString());
	}
	protected List<Record> parserData(List<RocketmqMessage<Object>> messages) {
		List<Record> results = new ArrayList<Record>();
		for(RocketmqMessage<Object> consumerRecord:messages) {
			deserializeData(consumerRecord,results);

		}
		return results;
	}



}
