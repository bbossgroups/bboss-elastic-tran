package org.frameworkset.tran.kafka.output.filelog;
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

import org.frameworkset.plugin.kafka.KafkaProductor;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.input.file.FileBaseDataTranPlugin;
import org.frameworkset.tran.kafka.output.KafkaOutputContext;
import org.frameworkset.tran.kafka.output.KafkaOutputDataTran;
import org.frameworkset.tran.kafka.output.KafkaSend;
import org.frameworkset.tran.kafka.output.KafkaSendImpl;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;

import java.util.concurrent.CountDownLatch;

/**
 * <p>Description: 采集日志数据并发送到Kafka插件，支持增量和全量导入</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/3/29 11:25
 * @author yin-bp@163.com
 * @version 1.0
 */
public class FileLog2KafkaDataTranPlugin extends FileBaseDataTranPlugin implements KafkaSend {
	private KafkaProductor kafkaProductor;
	private KafkaOutputContext kafkaOutputContext;
	public FileLog2KafkaDataTranPlugin(ImportContext importContext, ImportContext targetImportContext) {
		super(importContext, targetImportContext);
		kafkaOutputContext = (KafkaOutputContext) targetImportContext;
	}

	@Override
	protected BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, Status currentStatus) {

		KafkaOutputDataTran kafkaOutputDataTran = new KafkaOutputDataTran(  taskContext,jdbcResultSet,importContext,   targetImportContext,(CountDownLatch)null,  currentStatus);
		kafkaOutputDataTran.init();
		return kafkaOutputDataTran;

	}


	@Override
	public void send(final TaskCommand taskCommand, TaskContext taskContext, Object key, Object data) {
		if(kafkaProductor == null){
			synchronized (this) {
				if(kafkaProductor == null) {

					kafkaProductor = KafkaSendImpl.buildProducer( kafkaOutputContext);
				}
			}
		}
		KafkaSendImpl.send(kafkaProductor,kafkaOutputContext,taskCommand,taskContext,key,data);
	}
}
