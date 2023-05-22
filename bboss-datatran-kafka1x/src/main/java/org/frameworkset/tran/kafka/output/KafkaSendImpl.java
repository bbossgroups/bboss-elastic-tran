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
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.kafka.output.Kafka1OutputConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
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
	public static KafkaProductor buildProducer(Kafka1OutputConfig kafka1OutputConfig){
		KafkaProductor kafkaProductor = new KafkaProductor();
		kafkaProductor.setProductorPropes(kafka1OutputConfig.getKafkaConfigs());
		kafkaProductor.setSendDatatoKafka(true);
		kafkaProductor.init();
		return kafkaProductor;
	}
	public static void send(KafkaProductor kafkaProductor,Kafka1OutputConfig kafka1OutputConfig,final TaskCommand taskCommand, TaskContext taskContext, Object key, Object data) {

		Callback callback = new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				Date endTime = new Date();
				ImportContext importContext = taskCommand.getImportContext();
				ImportCount importCount = taskCommand.getImportCount();
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				if(exception == null) {
					taskCommand.finishTask();
					long[] metrics = importCount.increamentSuccessCount((long)taskCommand.getDataSize());
					taskMetrics.setTotalSuccessRecords(metrics[0]);
					taskMetrics.setTotalRecords(metrics[1]);
					taskMetrics.setSuccessRecords((long)taskCommand.getDataSize());
					long ignoreTotalCount = importCount.getIgnoreTotalCount();
					taskMetrics.setIgnoreRecords(ignoreTotalCount - taskMetrics.getTotalIgnoreRecords());
					taskMetrics.setTotalIgnoreRecords(ignoreTotalCount);
					taskMetrics.setTaskEndTime(endTime);
					if (importContext.getExportResultHandler() != null) {//处理返回值
						try {
							importContext.getExportResultHandler().handleResult(taskCommand, metadata);
						}
						catch (Exception e){
							logger.warn("",e);
						}
					}
//					exportResultHandler.success(taskCommand, metadata);
				}
				else{
//					exportResultHandler.exception(taskCommand,new KafkaSendException(metadata,exception));
					long[] metrics = importCount.increamentFailedCount(taskCommand.getDataSize());
					taskMetrics.setFailedRecords(taskCommand.getDataSize());
					taskMetrics.setTotalRecords(metrics[1]);
					taskMetrics.setTotalFailedRecords(metrics[0]);
					long ignoreTotalCount = importCount.getIgnoreTotalCount();
					taskMetrics.setIgnoreRecords(ignoreTotalCount - taskMetrics.getTotalIgnoreRecords());
					taskMetrics.setTotalIgnoreRecords(ignoreTotalCount);
					taskMetrics.setTaskEndTime(endTime);
					if (importContext.getExportResultHandler() != null) {
						try {
							importContext.getExportResultHandler().handleException(taskCommand,new KafkaSendException(metadata,exception));
						}
						catch (Exception ee){
							logger.warn("",ee);
						}
					}
//					throw new ElasticSearchException(e);
				}
			}
		};

		Future<RecordMetadata> future = kafkaProductor.send(kafka1OutputConfig.getTopic(),key,data,callback);
		if(!kafka1OutputConfig.isKafkaAsynSend()){
			try {
				future.get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				throw new DataImportException(e.getCause() != null?e.getCause():e);
			}
		}
	}
}
