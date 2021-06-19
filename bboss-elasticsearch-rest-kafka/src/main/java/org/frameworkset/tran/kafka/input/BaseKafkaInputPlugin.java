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

import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.kafka.KafkaContext;
import org.frameworkset.tran.kafka.KafkaResultSet;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseKafkaInputPlugin extends BaseDataTranPlugin implements DataTranPlugin {
	protected KafkaContext kafkaContext;
	protected void init(ImportContext importContext,ImportContext targetImportContext){
		super.init(importContext,  targetImportContext);
		kafkaContext = (KafkaContext)importContext;

	}

	@Override
	public void initStatusTableId() {

	}
	public BaseKafkaInputPlugin(ImportContext importContext,ImportContext targetImportContext){
		super(  importContext,  targetImportContext);


	}

	@Override
	public void importData() throws ESDataImportException {


			long importStartTime = System.currentTimeMillis();
			this.doImportData(null);
			long importEndTime = System.currentTimeMillis();
			if( isPrintTaskLog())
				logger.info(new StringBuilder().append("Execute job Take ").append((importEndTime - importStartTime)).append(" ms").toString());


	}

	@Override
	public void beforeInit() {


	}


	@Override
	public void afterInit(){
	}

	protected abstract void initKafkaTranBatchConsumer2ndStore(BaseDataTran kafka2ESDataTran) throws Exception;

	protected abstract BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, Status currentStatus);

	public void doImportData(TaskContext taskContext)  throws ESDataImportException{
		KafkaResultSet kafkaResultSet = new KafkaResultSet(this.importContext);
//		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final BaseDataTran kafka2ESDataTran = createBaseDataTran( taskContext,kafkaResultSet,  currentStatus);
		this.setHasTran();
		Thread tranThread = null;
		try {
			tranThread = new Thread(new Runnable() {
				@Override
				public void run() {
					kafka2ESDataTran.tran();
				}
			},"kafka-elasticsearch-Tran");
			tranThread.start();

			this.initKafkaTranBatchConsumer2ndStore(kafka2ESDataTran);
		} catch (ESDataImportException e) {
			throw e;
		} catch (Exception e) {
			throw new ESDataImportException(e);
		}
		finally {
//			kafkaResultSet.reachEend();
//			try {
//				countDownLatch.await();
//			} catch (InterruptedException e) {
//				if(logger.isErrorEnabled())
//					logger.error("",e);
//			}
		}

	}
	@Override
	public void initSchedule(){
		logger.info("Ignore initSchedule for plugin {}",this.getClass().getName());
	}
	@Override
	public void initLastValueClumnName(){
		setIncreamentImport(false);
	}
}
