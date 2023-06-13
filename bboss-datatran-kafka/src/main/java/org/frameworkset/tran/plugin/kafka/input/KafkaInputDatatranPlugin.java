package org.frameworkset.tran.plugin.kafka.input;
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

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.kafka.KafkaResultSet;
import org.frameworkset.tran.plugin.BaseInputPlugin;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class KafkaInputDatatranPlugin extends BaseInputPlugin  {
	private static final Logger logger = LoggerFactory.getLogger(KafkaInputDatatranPlugin.class);
	protected KafkaInputConfig kafkaInputConfig;

	@Override
	public void init(){


	}
    public boolean isEventMsgTypePlugin(){
        return true;
    }
	@Override
	public void initStatusTableId() {

	}
	public KafkaInputDatatranPlugin(ImportContext importContext){
		super(  importContext);
		kafkaInputConfig = (KafkaInputConfig) importContext.getInputConfig();


	}


	@Override
	public void beforeInit() {


	}


	@Override
	public void afterInit(){
	}

	protected abstract void initKafkaTranBatchConsumer2ndStore(BaseDataTran kafka2ESDataTran) throws Exception;


    @Override
	public void doImportData(TaskContext taskContext)  throws DataImportException {
		KafkaResultSet kafkaResultSet = new KafkaResultSet(this.importContext);
//		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final BaseDataTran kafka2ESDataTran = dataTranPlugin.createBaseDataTran( taskContext,kafkaResultSet,null,dataTranPlugin.getCurrentStatus());
//		dataTranPlugin.setHasTran();
		Thread tranThread = null;
		try {
			tranThread = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						kafka2ESDataTran.tran();
						dataTranPlugin.afterCall(taskContext);
					}
					catch (DataImportException dataImportException){
						logger.error("",dataImportException);
						dataTranPlugin.throwException(  taskContext,  dataImportException);
                        kafka2ESDataTran.stop();
                        importContext.finishAndWaitTran(dataImportException);
					}
					catch (RuntimeException dataImportException){
						logger.error("",dataImportException);
						dataTranPlugin.throwException(  taskContext,  dataImportException);
                        kafka2ESDataTran.stop();
                        importContext.finishAndWaitTran(dataImportException);
					}
					catch (Throwable dataImportException){
						logger.error("",dataImportException);
						DataImportException dataImportException_ = new DataImportException(dataImportException);
						dataTranPlugin.throwException(  taskContext, dataImportException_);
                        kafka2ESDataTran.stop();
                        importContext.finishAndWaitTran(dataImportException);
					}
				}
			},"kafka-input-dataTran");
			tranThread.start();

			this.initKafkaTranBatchConsumer2ndStore(kafka2ESDataTran);
		} catch (DataImportException e) {
            kafka2ESDataTran.stop();
			throw e;
		} catch (Exception e) {
            kafka2ESDataTran.stop();
			throw new DataImportException(e);
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

}
