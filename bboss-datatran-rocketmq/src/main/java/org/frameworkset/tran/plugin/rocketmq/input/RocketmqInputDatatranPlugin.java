package org.frameworkset.tran.plugin.rocketmq.input;
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
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.plugin.BaseInputPlugin;
import org.frameworkset.tran.rocketmq.RocketmqResultSet;
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
public class RocketmqInputDatatranPlugin extends BaseInputPlugin  {
	private static final Logger logger = LoggerFactory.getLogger(RocketmqInputDatatranPlugin.class);
	protected RocketmqInputConfig rocketmqInputConfig;
    private RocketmqTranConsumer2ndStore rocketmqTranConsumer2ndStore;
	@Override
	public void init(){


	}
    public boolean isEventMsgTypePlugin(){
        return true;
    }
	@Override
	public void initStatusTableId() {

	}
	public RocketmqInputDatatranPlugin(ImportContext importContext){
		super(  importContext);
		rocketmqInputConfig = (RocketmqInputConfig) importContext.getInputConfig();
        this.jobType = "RocketmqInputDatatranPlugin";

	}
    @Override
    public void destroy(boolean waitTranStop) {

    }

	@Override
	public void beforeInit() {


	}


	@Override
	public void afterInit(){
	}


    protected void initRocketmqTranBatchConsumer2ndStore(BaseDataTran baseDataTran) throws Exception {
        final RocketmqTranConsumer2ndStore rocketmqTranConsumer2ndStore = new RocketmqTranConsumer2ndStore(baseDataTran,rocketmqInputConfig);
        rocketmqTranConsumer2ndStore.setTopic(rocketmqInputConfig.getTopic());
        Integer maxPollRecords = rocketmqInputConfig.getMaxPollRecords();
        if(maxPollRecords == null) {
            maxPollRecords = importContext.getFetchSize();
        }
        rocketmqTranConsumer2ndStore.setMaxPollRecords(maxPollRecords);
        rocketmqTranConsumer2ndStore.setConsumeMessageBatchMaxSize(rocketmqInputConfig.getConsumeMessageBatchMaxSize() == null
                ?maxPollRecords:rocketmqInputConfig.getConsumeMessageBatchMaxSize());

        rocketmqTranConsumer2ndStore.setConsumerPropes(rocketmqInputConfig.getConsumerConfigs());
        rocketmqTranConsumer2ndStore.setWorkThreads(rocketmqInputConfig.getWorkThreads() == null?5:rocketmqInputConfig.getWorkThreads());



        rocketmqTranConsumer2ndStore.setKeyDeserializer(rocketmqInputConfig.getKeyDeserializer());
        rocketmqTranConsumer2ndStore.setValueDeserializer(rocketmqInputConfig.getValueDeserializer());

        rocketmqTranConsumer2ndStore.setConsumerGroup(rocketmqInputConfig.getConsumerGroup());
        rocketmqTranConsumer2ndStore.setAccessKey(rocketmqInputConfig.getAccessKey());
        rocketmqTranConsumer2ndStore.setSecretKey(rocketmqInputConfig.getSecretKey());
        rocketmqTranConsumer2ndStore.setSignature(rocketmqInputConfig.getSignature());
        rocketmqTranConsumer2ndStore.setSecurityToken(rocketmqInputConfig.getSecurityToken());
        rocketmqTranConsumer2ndStore.setTag(rocketmqInputConfig.getTag());
        rocketmqTranConsumer2ndStore.setEnableSsl(rocketmqInputConfig.getEnableSsl());
        rocketmqTranConsumer2ndStore.setNamesrvAddr(rocketmqInputConfig.getNamesrvAddr());
        rocketmqTranConsumer2ndStore.setConsumeFromWhere(rocketmqInputConfig.getConsumeFromWhere());
        rocketmqTranConsumer2ndStore.setConsumeTimestamp(rocketmqInputConfig.getConsumeTimestamp());        
        
        rocketmqTranConsumer2ndStore.afterPropertiesSet();
        rocketmqTranConsumer2ndStore.run(false);
        this.rocketmqTranConsumer2ndStore = rocketmqTranConsumer2ndStore;
    }
    @Override
    public void stopCollectData(){
        try {
            if (rocketmqTranConsumer2ndStore != null) {
                rocketmqTranConsumer2ndStore.shutdown();
            }
        }
        catch (Exception e){
            logger.warn("",e);
        }
        super.stopCollectData();
    }


    @Override
	public void doImportData(TaskContext taskContext)  throws DataImportException {
		RocketmqResultSet rocketmqResultSet = new RocketmqResultSet(this.importContext);
//		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final BaseDataTran baseDataTran = dataTranPlugin.createBaseDataTran( taskContext, rocketmqResultSet,null,dataTranPlugin.getCurrentStatus());
//		dataTranPlugin.setHasTran();
		Thread tranThread = null;
		try {
			tranThread = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						baseDataTran.tran();
						dataTranPlugin.afterCall(taskContext);
					}
					catch (DataImportException dataImportException){
						logger.error("",dataImportException);
						dataTranPlugin.throwException(  taskContext,  dataImportException);
                        baseDataTran.stop2ndClearResultsetQueue(true);
                        importContext.finishAndWaitTran(dataImportException);
					}
					catch (RuntimeException dataImportException){
						logger.error("",dataImportException);
						dataTranPlugin.throwException(  taskContext,  dataImportException);
                        baseDataTran.stop2ndClearResultsetQueue(true);
                        importContext.finishAndWaitTran(dataImportException);
					}
					catch (Throwable dataImportException){
						logger.error("",dataImportException);
						DataImportException dataImportException_ = ImportExceptionUtil.buildDataImportException(importContext,dataImportException);
						dataTranPlugin.throwException(  taskContext, dataImportException_);
                        baseDataTran.stop2ndClearResultsetQueue(true);
                        importContext.finishAndWaitTran(dataImportException);
					}
				}
			},"Rocketmq-input-dataTran");
			tranThread.start();

			this.initRocketmqTranBatchConsumer2ndStore(baseDataTran);
		} catch (DataImportException e) {
            baseDataTran.stop2ndClearResultsetQueue(true);
			throw e;
		} catch (Exception e) {
            baseDataTran.stop2ndClearResultsetQueue(true);
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}
		

	}

}
