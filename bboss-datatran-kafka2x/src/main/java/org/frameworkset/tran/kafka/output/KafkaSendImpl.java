package org.frameworkset.tran.kafka.output;
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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.frameworkset.plugin.kafka.KafkaProductor;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.kafka.output.KafkaOutputConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/28 15:41
 * @author biaoping.yin
 * @version 1.0
 */
public class KafkaSendImpl {
	private static Logger logger = LoggerFactory.getLogger(KafkaSendImpl.class);

	public static KafkaProductor buildProducer(KafkaOutputConfig kafkaOutputConfig){
		KafkaProductor kafkaProductor = new KafkaProductor();
		kafkaProductor.setProductorPropes(kafkaOutputConfig.getKafkaConfigs());
		kafkaProductor.setSendDatatoKafka(true);
		kafkaProductor.init();
		return kafkaProductor;
	}
	 

    public static void batchSend(KafkaProductor kafkaProductor, KafkaOutputConfig kafkaOutputConfig, final BaseTaskCommand taskCommand, TaskContext taskContext,
                            String topic, Object key, Object data) {

        Callback callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                ImportContext importContext = taskCommand.getImportContext();
//                ImportCount importCount = taskCommand.getImportCount();
//                TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                if(exception != null) {
                    TaskCall.exportResultHandler( taskCommand,importContext,new KafkaSendException(metadata,exception));
                }

            }
        };

        Future<RecordMetadata> future = kafkaProductor.send(topic,key,data,callback);
        if(!kafkaOutputConfig.isKafkaAsynSend()){
            try {
                future.get();
            } catch (InterruptedException e) {
                logger.warn("InterruptedException",e);
            } catch (ExecutionException e) {
                throw new DataImportException(e.getCause() != null?e.getCause():e);
            }
        }
    }
}
