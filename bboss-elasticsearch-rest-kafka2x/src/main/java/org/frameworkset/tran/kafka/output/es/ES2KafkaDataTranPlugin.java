package org.frameworkset.tran.kafka.output.es;
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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.frameworkset.plugin.kafka.KafkaProductor;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.es.input.ESInputPlugin;
import org.frameworkset.tran.kafka.output.KafkaOutputContext;
import org.frameworkset.tran.kafka.output.KafkaOutputDataTran;
import org.frameworkset.tran.kafka.output.KafkaSend;
import org.frameworkset.tran.kafka.output.KafkaSendException;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class ES2KafkaDataTranPlugin extends ESInputPlugin implements DataTranPlugin, KafkaSend {
	private KafkaProductor kafkaProductor;
	private KafkaOutputContext kafkaOutputContext;
	protected void init(ImportContext importContext,ImportContext targetImportContext){
		super.init(importContext,targetImportContext);
		kafkaOutputContext = (KafkaOutputContext) targetImportContext;
	}
	@Override
	protected BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, CountDownLatch countDownLatch){
		KafkaOutputDataTran kafkaOutputDataTran = new KafkaOutputDataTran(  taskContext,jdbcResultSet,importContext,   targetImportContext,countDownLatch);
		kafkaOutputDataTran.init();
		return kafkaOutputDataTran;
	}
	@Override
	protected void doBatchHandler(TaskContext taskContext){

	}

	public ES2KafkaDataTranPlugin(ImportContext importContext, ImportContext targetImportContext){
		super(  importContext,  targetImportContext);


	}

	@Override
	public void beforeInit() {
		super.beforeInit();
//		this.initDS(importContext.getDbConfig());
//		initOtherDSes(importContext.getConfigs());


	}


	@Override
	public void send(final TaskCommand taskCommand,TaskContext taskContext, Object key, Object data, final ExportResultHandler exportResultHandler) {
		if(kafkaProductor == null){
			synchronized (this) {
				if(kafkaProductor == null) {
					kafkaProductor = new KafkaProductor();
					kafkaProductor.setProductorPropes(kafkaOutputContext.getKafkaConfigs());
					kafkaProductor.setSendDatatoKafka(true);
					kafkaProductor.init();
				}
			}
		}
		Callback callback = new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				ImportContext importContext = taskCommand.getImportContext();
				ImportCount importCount = taskCommand.getImportCount();
				TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
				if(exception == null) {
					taskCommand.finishTask();
					long[] metrics = importCount.increamentSuccessCount((long)taskCommand.getDataSize());
					taskMetrics.setTotalSuccessRecords(metrics[0]);
					taskMetrics.setTotalRecords(metrics[1]);
					taskMetrics.setSuccessRecords((long)taskCommand.getDataSize());
					taskMetrics.setTotalIgnoreRecords(importCount.getIgnoreTotalCount());
					taskMetrics.setTaskEndTime(new Date());
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
					taskMetrics.setTotalIgnoreRecords(importCount.getIgnoreTotalCount());
					taskMetrics.setTaskEndTime(new Date());
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

		Future<RecordMetadata> future = kafkaProductor.send(kafkaOutputContext.getTopic(),key,data,callback);
		if(!kafkaOutputContext.kafkaAsynSend()){
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
