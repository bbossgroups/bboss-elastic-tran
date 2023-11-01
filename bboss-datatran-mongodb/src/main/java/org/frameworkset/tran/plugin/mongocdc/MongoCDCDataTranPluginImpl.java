package org.frameworkset.tran.plugin.mongocdc;
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

import com.frameworkset.orm.annotation.BatchContext;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ContextImpl;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.*;
import org.frameworkset.tran.status.BaseStatusManager;
import org.frameworkset.tran.status.InitLastValueClumnName;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.status.SetLastValueType;
import org.frameworkset.tran.util.StoppedThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.frameworkset.tran.metrics.job.MetricsConfig.DEFAULT_metricsInterval;


/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 16:55
 * @author biaoping.yin
 * @version 1.0
 */
public class MongoCDCDataTranPluginImpl extends DataTranPluginImpl {

	protected static Logger logger = LoggerFactory.getLogger(MongoCDCDataTranPluginImpl.class);

	private StoppedThread metricsThread;
    private MongoCDCInputConfig mongoCDCInputConfig;

	/**
	 * 默认十分钟执行一次拦截器监控数据afterCall方法
	 */
	private long metricsInterval ;
	public MongoCDCDataTranPluginImpl(ImportContext importContext){
		super(importContext);
        mongoCDCInputConfig = (MongoCDCInputConfig)importContext.getInputConfig();
		metricsInterval = mongoCDCInputConfig.getMetricsInterval();
		if(metricsInterval <= 0L){
			metricsInterval = DEFAULT_metricsInterval;
		}
	}
    @Override
    public boolean isSingleLastValueType(){
        return false;
    }
    @Override
    public SetLastValueType getSetLastValueType(){
        return new SetLastValueType (){

            public void set(){
                if( mongoCDCInputConfig.isEnableIncrement()) {
                    importContext.setLastValueType(ImportIncreamentConfig.STRING_TYPE);
                    statusManager.initLastValueType();
                }
            }
        };
    }

//    @Override
//    public boolean onlyUseBatchExecute(){
//        return true;
//    }
    @Override
    public boolean useFilePointer(){
        return true;
    }
    @Override
    protected InitLastValueClumnName getInitLastValueClumnName(){
        return new InitLastValueClumnName (){

            public void initLastValueClumnName(){
                if(!mongoCDCInputConfig.isEnableIncrement()){
                    statusManager.setIncreamentImport(false);
                }
//                if(!SimpleStringUtil.isEmpty(mongoCDCInputConfig.getFileNames())){
//                    statusManager.setIncreamentImport(false);
//                }
//                else if(mongoCDCInputConfig.isCollectMasterHistoryBinlog()){
//                    statusManager.setIncreamentImport(false);
//                }
//                else if(!mongoCDCInputConfig.isEnableIncrement()){
//                    statusManager.setIncreamentImport(false);
//                }
//                else {
//                    if (mongoCDCInputConfig.getPosition() == null) {
//                        mongoCDCInputConfig.setPosition(0L);
//                    }
//                }
            }
        };
    }

    @Override
    public Context buildContext(TaskContext taskContext, TranResultSet tranResultSet, BatchContext batchContext){
        ContextImpl context = new ContextImpl(  taskContext,importContext, tranResultSet, batchContext);
        context.setAction(tranResultSet.getAction());
        return context;
    }

    public boolean neadFinishJob(){
//        return SimpleStringUtil.isNotEmpty(this.mongoCDCInputConfig.getFileNames())
//                || mongoCDCInputConfig.isCollectMasterHistoryBinlog();
        return false;
    }
	@Override
	public void importData(ScheduleEndCall scheduleEndCall) throws DataImportException {


		long importStartTime = System.currentTimeMillis();
        MongoCDCTaskContext taskContext = inputPlugin.isEnablePluginTaskIntercept()?new MongoCDCTaskContext(importContext):null;
		try {
			preCall(taskContext);
			this.doImportData(taskContext);
			List<CallInterceptor> callInterceptors = importContext.getCallInterceptors();
			if(callInterceptors != null && callInterceptors.size() > 0) {
				metricsThread = new StoppedThread() {
					@Override
					public void run() {
						do {
							if (stopped) {
								break;
							}
							try {
								taskContext.reInitContext(new MongoCDCTaskContext.ReInitAction(){

									@Override
									public void afterCall(TaskContext taskContext) {
                                        MongoCDCDataTranPluginImpl.this.afterCall(taskContext);
									}

									@Override
									public void preCall(TaskContext taskContext) {
                                        MongoCDCDataTranPluginImpl.this.preCall(taskContext);
									}
								});

							} catch (Exception e) {
								logger.error("MysqlBinlogDataTranPlugin-MetricsThread  afterCall Exception", e);
							}
							if (stopped) {
								break;
							}
							try {
								sleep(metricsInterval);
							} catch (InterruptedException e) {
								logger.error("MysqlBinlogDataTranPlugin-MetricsThread  InterruptedException", e);
								break;
							}

						} while (true);
					}
				};
				metricsThread.setName("MysqlBinlogDataTranPlugin-MetricsThread");
				metricsThread.setDaemon(true);
				metricsThread.start();
			}

			long importEndTime = System.currentTimeMillis();
			if (isPrintTaskLog())
				logger.info(new StringBuilder().append("Execute job Take ").append((importEndTime - importStartTime)).append(" ms").toString());
            if(neadFinishJob()){//如果是采集binlog文件的情况下，需要结束作业
                importContext.finishAndWaitTran(null);
            }
		}
		catch (DataImportException dataImportException){
			throwException(taskContext,dataImportException);
            importContext.finishAndWaitTran(dataImportException);
        }
		catch (Exception dataImportException){
			throwException(taskContext,dataImportException);
            importContext.finishAndWaitTran(dataImportException);
            throw dataImportException;
		}
		catch (Throwable dataImportException){
			DataImportException e = new DataImportException(dataImportException);
			throwException(taskContext,new DataImportException(dataImportException));
            importContext.finishAndWaitTran(dataImportException);
            throw e;
		}


	}

	public void afterCall(TaskContext taskContext){
		super.afterCall(taskContext);
	}
	@Override
	public void initSchedule(){
		logger.info("Ignore initSchedule for plugin {}",this.getClass().getName());
	}


	@Override
	public void destroy(DestroyPolicy destroyPolicy) {
        if(checkTranToStop()){
            return;
        }
		if(metricsThread != null)
			metricsThread.stopThread();
		super.destroy(destroyPolicy);
	}
	//	@Override
//	public void initLastValueClumnName(){
//		setIncreamentImport(false);
//	}
    @Override
    public boolean needUpdateLastValueWrapper(Integer lastValueType, LastValueWrapper oldValue,LastValueWrapper newValue){
//        if(newValue == null)
//            return false;
//        if(!oldValue.getStrLastValue().equals(newValue.getStrLastValue())){
//            if(oldValue.getTimeStamp() < newValue.getTimeStamp()){
//                return true;
//            }
//            else{
//                return false;
//            }
//        }
//        else {
//            return BaseStatusManager.needUpdate(lastValueType, oldValue.getLastValue(), newValue.getLastValue());
//        }
        if(newValue == null)
            return false;
        if(oldValue == null)
            return true;
//        if(oldValue.getStrLastValue() == null && newValue.getStrLastValue() == null){
//
//            return max( oldValue.getLastValue(), newValue.getLastValue());
//        }
//        else if( oldValue.getStrLastValue() == null || newValue.getStrLastValue() == null ){
//            return true;
//        }
//
//        else {
//            if (!oldValue.getStrLastValue().equals(newValue.getStrLastValue())) {
//                if (oldValue.getTimeStamp() < newValue.getTimeStamp()) {
//                    return true;
//                } else {
//                    return false;
//                }
//            } else {
//                return max( oldValue.getLastValue(), newValue.getLastValue());
//            }
//        }
        if (oldValue.getTimeStamp() < newValue.getTimeStamp()) {
            return true;
        } else {
            return false;
        }
    }
    /**
     * Number ts = (Number)lastValue.getLastValue();
     * 				Number nts = (Number)taskMetrics.getLastValue().getLastValue();
     * 				if(nts.longValue() > ts.longValue())
     * 					this.lastValue = taskMetrics.getLastValue();
     * @param oldValue
     * @param newValue
     * @return
     */
    @Override
    public LastValueWrapper maxNumberLastValue(LastValueWrapper oldValue, LastValueWrapper newValue){
        if (oldValue.getTimeStamp() < newValue.getTimeStamp()) {
            return newValue;
        } else {
            return oldValue;
        }
//        if(oldValue.getStrLastValue() == null && newValue.getStrLastValue() == null){
//
//            return compareValue( oldValue,  newValue);
//        }
//        else if( oldValue.getStrLastValue() == null || newValue.getStrLastValue() == null ){
//            return newValue;
//        }
//
//        else {
//            if (!oldValue.getStrLastValue().equals(newValue.getStrLastValue())) {
//                if (oldValue.getTimeStamp() < newValue.getTimeStamp()) {
//                    return newValue;
//                } else {
//                    return oldValue;
//                }
//            } else {
//                return compareValue( oldValue,  newValue);
//            }
//        }
    }
    @Override
    public LastValueWrapper maxLastValue(LastValueWrapper oldValue, BaseDataTran baseDataTran){
        LastValueWrapper newValue = baseDataTran.getLastValueWrapper();

        return maxNumberLastValue( oldValue,  newValue);


    }


    @Override
    public void initLastValueStatus(Status currentStatus, BaseStatusManager baseStatusManager) throws Exception {

        LastValueWrapper lastValueWrapper = currentStatus.getCurrentLastValueWrapper();


        if (mongoCDCInputConfig.getPosition() != null) {

            lastValueWrapper.setLastValue(mongoCDCInputConfig.getPosition());
        }
        else if (importContext.getConfigLastValue() != null) {

            lastValueWrapper.setLastValue(importContext.getConfigLastValue());
        }
        if(mongoCDCInputConfig.getLastTimeStamp() != null){
            lastValueWrapper.setTimeStamp(mongoCDCInputConfig.getLastTimeStamp());
        }
//        else {
//            lastValueWrapper.setLastValue(0l);
//        }
//
//        if (SimpleStringUtil.isNotEmpty(mongoCDCInputConfig.getMastterBinLogFile())) {
//
//            lastValueWrapper.setStrLastValue(mongoCDCInputConfig.getMastterBinLogFile());
//        }



//            lastValueWrapper.setLastValue(currentStatus.getLastValue());


    }
}
