package org.frameworkset.tran.kafka.output.hbase;
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
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.hbase.HBaseInputPlugin;
import org.frameworkset.tran.hbase.HBaseResultSet;
import org.frameworkset.tran.kafka.output.KafkaOutputContext;
import org.frameworkset.tran.kafka.output.KafkaOutputDataTran;
import org.frameworkset.tran.kafka.output.KafkaSend;
import org.frameworkset.tran.kafka.output.KafkaSendImpl;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;

import java.util.concurrent.CountDownLatch;

/**
 * <p>Description: hbase to kafka data tran plugin.</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/5/30 11:04
 * @author biaoping.yin
 * @version 1.0
 */
public class HBase2KafkaDataTranPlugin extends HBaseInputPlugin implements DataTranPlugin, KafkaSend {
	private KafkaProductor kafkaProductor;
	private KafkaOutputContext kafkaOutputContext;
	@Override
	public void init(ImportContext importContext, ImportContext targetImportContext){
		super.init(importContext,targetImportContext);
		kafkaOutputContext = (KafkaOutputContext) targetImportContext;
	}



	public HBase2KafkaDataTranPlugin(ImportContext importContext, ImportContext targetImportContext){
		super(  importContext,  targetImportContext);


	}

	@Override
	protected void doTran(org.apache.hadoop.hbase.client.ResultScanner rs, TaskContext taskContext) {
		HBaseResultSet hBaseResultSet = new HBaseResultSet(importContext,rs);
		KafkaOutputDataTran kafkaOutputDataTran = new KafkaOutputDataTran(  taskContext,hBaseResultSet,importContext,   targetImportContext,(CountDownLatch)null,  currentStatus);
		kafkaOutputDataTran.initTran();
		kafkaOutputDataTran.tran();
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
