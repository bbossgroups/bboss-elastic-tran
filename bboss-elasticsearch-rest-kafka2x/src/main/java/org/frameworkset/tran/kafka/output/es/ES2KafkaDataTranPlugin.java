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

import org.frameworkset.plugin.kafka.KafkaProductor;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.es.input.ESInputPlugin;
import org.frameworkset.tran.kafka.output.KafkaOutputContext;
import org.frameworkset.tran.kafka.output.KafkaOutputDataTran;
import org.frameworkset.tran.kafka.output.KafkaSend;
import org.frameworkset.tran.kafka.output.KafkaSendImpl;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;

import java.util.concurrent.CountDownLatch;

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
	public void send(final TaskCommand taskCommand,TaskContext taskContext, Object key, Object data) {
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
		KafkaSendImpl.send(kafkaProductor,kafkaOutputContext,taskCommand,taskContext,key,data);

	}
}
