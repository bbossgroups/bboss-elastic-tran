package org.frameworkset.tran.plugin.kafka.output;
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

import org.frameworkset.plugin.kafka.KafkaProductor;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.kafka.output.*;
import org.frameworkset.tran.metrics.JobTaskMetrics;
import org.frameworkset.tran.plugin.BasePlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class Kafka2OutputDataTranPlugin extends BasePlugin implements OutputPlugin, KafkaSend {
	private KafkaProductor kafkaProductor;
	private Kafka2OutputConfig kafka2OutputConfig;
	@Override
	public void init(){

	}

	@Override
	public void destroy(boolean waitTranStop) {
		if(kafkaProductor != null){
			kafkaProductor.destroy();
		}
	}

	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, JobCountDownLatch countDownLatch, Status currentStatus){
		KafkaOutputDataTran kafkaOutputDataTran = new KafkaOutputDataTran(  taskContext,jdbcResultSet, importContext,countDownLatch,  currentStatus);
		kafkaOutputDataTran.initTran();
		return kafkaOutputDataTran;
	}


	public Kafka2OutputDataTranPlugin(ImportContext importContext){
		super(importContext);
		kafka2OutputConfig = (Kafka2OutputConfig) importContext.getOutputConfig();
	}

	@Override
	public void afterInit() {
		kafka2OutputConfig.setKafkaSend(this);
	}

	@Override
	public void beforeInit() {

	}


	@Override
	public void send(final KafkaCommand taskCommand, TaskContext taskContext, Object key, Object data) {
		if(kafkaProductor == null){
			synchronized (this) {
				if(kafkaProductor == null) {
					kafkaProductor = KafkaSendImpl.buildProducer( kafka2OutputConfig);

				}
			}
		}
		KafkaSendImpl.send(kafkaProductor,kafka2OutputConfig,taskCommand,taskContext,key,data);
	}
	@Override
	public JobTaskMetrics createJobTaskMetrics(){
//		return getOutputPlugin().createJobTaskMetrics();
		return new KafkaJobTaskMetrics();
	}
}
