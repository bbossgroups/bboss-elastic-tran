package org.frameworkset.tran.rocketmq.output;
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

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.frameworkset.rocketmq.RocketmqProductor;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.rocketmq.output.RocketmqOutputConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/28 15:41
 * @author biaoping.yin
 * @version 1.0
 */
public class RocketmqSendImpl {
	private static Logger logger = LoggerFactory.getLogger(RocketmqSendImpl.class);

	public static RocketmqProductor buildProducer(RocketmqOutputConfig rocketmqOutputConfig){
        RocketmqProductor rocketmqProductor = new RocketmqProductor();
        rocketmqProductor.setProductorPropes(rocketmqOutputConfig.getProductConfigs());
        rocketmqProductor.setSendDatatoRocketmq(true);

        rocketmqProductor.setProductGroup(rocketmqOutputConfig.getProductGroup());
        rocketmqProductor.setNamesrvAddr(rocketmqOutputConfig.getNamesrvAddr());
        rocketmqProductor.setValueCodecSerial(rocketmqOutputConfig.getValueCodecSerial());
        rocketmqProductor.setKeyCodecSerial(rocketmqOutputConfig.getKeyCodecSerial());

        rocketmqProductor.setAccessKey(rocketmqOutputConfig.getAccessKey());
        rocketmqProductor.setSecretKey(rocketmqOutputConfig.getSecretKey());
        rocketmqProductor.setSecurityToken(rocketmqOutputConfig.getSecurityToken());
        rocketmqProductor.setSignature(rocketmqOutputConfig.getSignature());


        rocketmqProductor.setEnableSsl(rocketmqOutputConfig.getEnableSsl());
        rocketmqProductor.init();
		return rocketmqProductor;
	}
    public static void send(RocketmqProductor rocketmqProductor, RocketmqOutputConfig rocketmqOutputConfig,  final BaseTaskCommand taskCommand, TaskContext taskContext,
                            String topic, Object key, Object data) {
        if(rocketmqOutputConfig.isRocketmqAsynSend()) {
            SendCallback callback = new SendCallback() {
                public void onException(final Throwable exception) {
                    ImportContext importContext = taskCommand.getImportContext();
                    ImportCount importCount = taskCommand.getImportCount();
                    TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                    TaskCall.handleException(new RocketmqSendException(exception), importCount, taskMetrics, taskCommand, importContext);
                }

                @Override
                public void onSuccess(final SendResult sendResult) {
                    Date endTime = new Date();
                    ImportContext importContext = taskCommand.getImportContext();
                    ImportCount importCount = taskCommand.getImportCount();
                    TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                    taskCommand.finishTask();
                    long[] metrics = importCount.increamentSuccessCount((long) taskCommand.getDataSize());
                    taskMetrics.setTotalSuccessRecords(metrics[0]);
                    taskMetrics.setTotalRecords(metrics[1]);
                    taskMetrics.setSuccessRecords((long) taskCommand.getDataSize());
                    taskMetrics.setRecords(taskMetrics.getSuccessRecords());
                    taskMetrics.setLastValue(taskCommand.getLastValue());
                    taskMetrics.setIgnoreRecords(importCount.getIgnoreTotalCount() - taskMetrics.getTotalIgnoreRecords());
                    long ignoreTotalCount = importCount.getIgnoreTotalCount();
                    taskMetrics.setIgnoreRecords(ignoreTotalCount - taskMetrics.getTotalIgnoreRecords());
                    taskMetrics.setTotalIgnoreRecords(ignoreTotalCount);
                    taskMetrics.setTaskEndTime(endTime);
                    if (importContext.getExportResultHandler() != null) {//处理返回值
                        try {
                            importContext.getExportResultHandler().handleResult(taskCommand, sendResult);
                        } catch (Exception e) {
                            logger.warn("", e);
                        }
                    }

                }
            };
            rocketmqProductor.send(topic, key, data, rocketmqOutputConfig.getTag(), callback);
        }
        else{
            try {
                rocketmqProductor.send(topic,key,data,rocketmqOutputConfig.getTag());
            }  catch (Exception e) {
                throw new DataImportException(e.getCause() != null?e.getCause():e);
            }
        }
       

        
    }  

    public static void batchSend(RocketmqProductor rocketmqProductor, RocketmqOutputConfig rocketmqOutputConfig, final BaseTaskCommand taskCommand, TaskContext taskContext,
                            String topic, Object key, Object data) {
       
        if(!rocketmqOutputConfig.isRocketmqAsynSend()){
            try {
                rocketmqProductor.send(topic,key,data,rocketmqOutputConfig.getTag());
            }  catch (Exception e) {
                throw new DataImportException(e.getCause() != null?e.getCause():e);
            }
        }
        else{
            SendCallback callback = new SendCallback() {
                public void onException(final Throwable exception){
                    ImportContext importContext = taskCommand.getImportContext();
                    ImportCount importCount = taskCommand.getImportCount();
                    TaskMetrics taskMetrics = taskCommand.getTaskMetrics();
                    TaskCall.handleException(new RocketmqSendException(exception),importCount,taskMetrics,taskCommand,importContext);
                }
                @Override
                public void  onSuccess(final SendResult sendResult) {
                }
            };
            rocketmqProductor.send(topic,key,data,rocketmqOutputConfig.getTag(),callback);
        }
    }
}
