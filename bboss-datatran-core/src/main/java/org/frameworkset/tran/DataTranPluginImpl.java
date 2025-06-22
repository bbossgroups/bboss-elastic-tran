package org.frameworkset.tran;
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

import com.frameworkset.common.poolman.util.DBConf;
import com.frameworkset.common.poolman.util.DBStartResult;
import com.frameworkset.common.poolman.util.SQLManager;
import com.frameworkset.common.poolman.util.SQLUtil;
import com.frameworkset.orm.annotation.BatchContext;
import com.frameworkset.util.SimpleStringUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.frameworkset.tran.config.DynamicParam;
import org.frameworkset.tran.config.DynamicParamContext;
import org.frameworkset.tran.config.JobInputParamGroup;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.schedule.ScheduleEndCall;
import org.frameworkset.tran.listener.JobClosedListener;
import org.frameworkset.tran.metrics.MetricsLogLevel;
import org.frameworkset.tran.metrics.MetricsLogReport;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.schedule.*;
import org.frameworkset.tran.schedule.timer.TimeUtil;
import org.frameworkset.tran.status.*;
import org.frameworkset.tran.util.TranConstant;
import org.frameworkset.tran.util.TranUtil;
import org.frameworkset.util.ResourceEnd;
import org.frameworkset.util.ResourceStart;
import org.frameworkset.util.ResourceStartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.sleep;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 16:55
 * @author biaoping.yin
 * @version 1.0
 */
public class DataTranPluginImpl implements DataTranPlugin {
	protected static Logger logger = LoggerFactory.getLogger(DataTranPluginImpl.class);

	protected InputPlugin inputPlugin ;
	protected OutputPlugin outputPlugin;
	private ExportCount exportCount;
	protected StatusManager statusManager;
	protected ScheduleAssert scheduleAssert;
	/**
	 * 包含所有启动成功的db数据源
	 */
	protected DBStartResult dbStartResult = new DBStartResult();
	public ExportCount getExportCount() {
		return exportCount;
	}
//    @Override
//    public boolean onlyUseBatchExecute(){
//        return false;
//    }
	@Override
	public boolean useFilePointer(){
		return false;
	}
	public InputPlugin getInputPlugin() {
		return inputPlugin;
	}

	public OutputPlugin getOutputPlugin() {
		return outputPlugin;
	}

    public StatusManager getStatusManager() {
        return statusManager;
    }

    @Override
	public ScheduleAssert getScheduleAssert() {
		return scheduleAssert;
	}

	public void setScheduleAssert(ScheduleAssert scheduleAssert){
		this.scheduleAssert = scheduleAssert;
	}
    protected LastValueWrapper compareValue(LastValueWrapper oldValue, LastValueWrapper newValue){
        if(max(oldValue.getLastValue(), newValue.getLastValue())){
            return newValue;
        }
        else{
            return oldValue;
        }
    }
    @Override
    public LastValueWrapper maxLastValue(LastValueWrapper oldValue, Record record){
        LastValueWrapper newValue = record.getLastValueWrapper();
        return compareValue(oldValue, newValue);
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
        return compareValue(oldValue, newValue);
    }

    protected boolean max(Object oldValue,Object newValue){

        return BaseStatusManager.max(importContext.getLastValueType(),oldValue,newValue);

    }

    @Override
	public Map getJobInputParams(TaskContext taskContext) {
		Map _params = importContext.getJobInputParams();
		Map params = new HashMap();
		if (_params != null && _params.size() > 0) {
			params.putAll(_params);
		}
		Map<String, DynamicParam> dynamicParams = importContext.getJobDynamicInputParams();
		if(dynamicParams == null || dynamicParams.size() == 0){
			return params;
		}
		Iterator<Map.Entry<String, DynamicParam>> iterator = dynamicParams.entrySet().iterator();
		DynamicParamContext dynamicParamContext = new DynamicParamContext();
		dynamicParamContext.setImportContext(importContext);
		dynamicParamContext.setTaskContext(taskContext);
		while (iterator.hasNext()){
			Map.Entry<String, DynamicParam> entry = iterator.next();
			Object value = null;
			try {
				value = entry.getValue().getValue(entry.getKey(),dynamicParamContext);
			} catch (DataImportException e) {
				throw e;
			} catch (Exception e) {
				throw ImportExceptionUtil.buildDataImportException(importContext.getOutputPlugin(),importContext,"get value of "+entry.getKey() + " failed:",e);
			}
			if(value != null)
				params.put(entry.getKey(),value);
		}
		return params;
	}

    public boolean hasJobInputParamGroups(){
        return importContext.getJobInputParamGroups() != null && importContext.getJobInputParamGroups().size() > 0;
    }
    public List<Map> getJobInputParamGroups(TaskContext taskContext) {
        List<JobInputParamGroup> jobInputParamGroups = importContext.getJobInputParamGroups();
        if(jobInputParamGroups == null){
            return null;
        }
        List<Map> _jobInputParamGroups = new ArrayList<>(jobInputParamGroups.size());
        for(JobInputParamGroup jobInputParamGroup : jobInputParamGroups) {
            Map _params = jobInputParamGroup.getJobInputParams();
            Map params = new HashMap();
            if (_params != null && _params.size() > 0) {
                params.putAll(_params);
            }
            Map<String, DynamicParam> dynamicParams = jobInputParamGroup.getJobDynamicInputParams();
            if (dynamicParams != null && dynamicParams.size() > 0) {

                Iterator<Map.Entry<String, DynamicParam>> iterator = dynamicParams.entrySet().iterator();
                DynamicParamContext dynamicParamContext = new DynamicParamContext();
                dynamicParamContext.setImportContext(importContext);
                dynamicParamContext.setTaskContext(taskContext);
                while (iterator.hasNext()) {
                    Map.Entry<String, DynamicParam> entry = iterator.next();
                    Object value = null;
                    try {
                        value = entry.getValue().getValue(entry.getKey(), dynamicParamContext);
                    } catch (DataImportException e) {
                        throw e;
                    } catch (Exception e) {
                        ImportExceptionUtil.buildDataImportException(importContext.getOutputPlugin(),importContext,"get value of " + entry.getKey() + " failed:", e);
                    }
                    if (value != null)
                        params.put(entry.getKey(), value);
                }
            }
            _jobInputParamGroups.add( params);
        }
        return _jobInputParamGroups;

    }

	public Map getJobInputParams(DynamicParamContext dynamicParamContext) {
		Map _params = importContext.getJobInputParams();
		Map params = new HashMap();
		if (_params != null && _params.size() > 0) {
			params.putAll(_params);
		}
		Map<String, DynamicParam> dynamicParams = importContext.getJobDynamicOutputParams();
		if(dynamicParams == null || dynamicParams.size() == 0){
			return params;
		}
		Iterator<Map.Entry<String, DynamicParam>> iterator = dynamicParams.entrySet().iterator();

		while (iterator.hasNext()){
			Map.Entry<String, DynamicParam> entry = iterator.next();
			Object value = null;
			try {
				value = entry.getValue().getValue(entry.getKey(),dynamicParamContext);
			} catch (DataImportException e) {
				throw e;
			} catch (Exception e) {
				throw ImportExceptionUtil.buildDataImportException(importContext.getOutputPlugin(),importContext,"get value of "+entry.getKey() + " failed:",e);
			}
			if(value != null)
				params.put(entry.getKey(),value);
		}
		return params;
	}

	public Map getJobOutputParams(TaskContext taskContext) {
		Map _params = importContext.getJobOutputParams();
		Map params = new HashMap();
		if (_params != null && _params.size() > 0) {
			params.putAll(_params);
		}
		Map<String, DynamicParam> dynamicParams = importContext.getJobDynamicOutputParams();
		if(dynamicParams == null || dynamicParams.size() == 0){
			return params;
		}
		Iterator<Map.Entry<String, DynamicParam>> iterator = dynamicParams.entrySet().iterator();
		DynamicParamContext dynamicParamContext = new DynamicParamContext();
		dynamicParamContext.setImportContext(importContext);
		dynamicParamContext.setTaskContext(taskContext);
		while (iterator.hasNext()){
			Map.Entry<String, DynamicParam> entry = iterator.next();
			Object value = null;
			try {
				value = entry.getValue().getValue(entry.getKey(),dynamicParamContext);
			} catch (DataImportException e) {
				throw e;
			} catch (Exception e) {
				throw ImportExceptionUtil.buildDataImportException(importContext.getOutputPlugin(),importContext,"get value of "+entry.getKey() + " failed:",e);
			}
			if(value != null)
				params.put(entry.getKey(),value);
		}
		return params;
	}

	public Map getJobOutputParams(DynamicParamContext dynamicParamContext) {
		Map _params = importContext.getJobOutputParams();
		Map params = new HashMap();
		if (_params != null && _params.size() > 0) {
			params.putAll(_params);
		}
		Map<String, DynamicParam> dynamicParams = importContext.getJobDynamicOutputParams();
		if(dynamicParams == null || dynamicParams.size() == 0){
			return params;
		}
		Iterator<Map.Entry<String, DynamicParam>> iterator = dynamicParams.entrySet().iterator();

		while (iterator.hasNext()){
			Map.Entry<String, DynamicParam> entry = iterator.next();
			Object value = null;
			try {
				value = entry.getValue().getValue(entry.getKey(),dynamicParamContext);
			} catch (DataImportException e) {
				throw e;
			} catch (Exception e) {
				throw ImportExceptionUtil.buildDataImportException(importContext.getOutputPlugin(),importContext,"get value of "+entry.getKey() + " failed:",e);
			}
			if(value != null)
				params.put(entry.getKey(),value);
		}
		return params;
	}

    /**
     * 如果作业处于暂停状态，回阻塞等待，直到consume使作业恢复执行，并返回true
     * @param autoPause
     * @return
     */
	public boolean isSchedulePaussed(boolean autoPause){
		if(this.scheduleAssert != null) {
            boolean paused = !this.scheduleAssert.assertSchedule(autoPause);
            if(paused){
                scheduleAssert.pausedAwait();
            }
        }
		return false;
	}
	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, JobCountDownLatch countDownLatch, Status currentStatus){
		return this.outputPlugin.createBaseDataTran(taskContext,tranResultSet,countDownLatch,currentStatus);
	}

    public void callTran(BaseDataTran baseDataTran){
        try {

            baseDataTran.tran(  );

        }
        catch (DataImportException dataImportException){
            baseDataTran.stop2ndClearResultsetQueue(true);
            throw dataImportException;
        }
        catch (RuntimeException dataImportException){
            baseDataTran.stop2ndClearResultsetQueue(true);
            throw  dataImportException;
        }
        catch (Throwable dataImportException){
            baseDataTran.stop2ndClearResultsetQueue(true);
            throw  dataImportException;
        }
    }

	@Override
	public void doImportData(TaskContext taskContext) {
        if(this.checkTranToStop())//任务处于停止状态，不再执行后续处理
        {
            return;
        }
		this.inputPlugin.doImportData(taskContext);
	}

	@Override
	public void addStatus(Status currentStatus) throws DataImportException {
        if(statusManager.isIncreamentImport())
		    statusManager.addStatus(currentStatus);
	}


	/**
	 * 识别任务是否已经完成
	 * @param status
	 * @return
	 */
    @Override
	public boolean isComplete(Status status){
		return status.getStatus() == ImportIncreamentConfig.STATUS_COMPLETE;
	}

	/**
	 * 识别任务对应的文件是否已经删除
	 * @param status
	 * @return
	 */
	public boolean isLostFile(Status status){
		return status.getStatus() == ImportIncreamentConfig.STATUS_LOSTFILE;
	}
	@Override
	public Context buildContext(TaskContext taskContext,  Record record,BatchContext batchContext){
//		return new ContextImpl(  taskContext,importContext, tranResultSet, batchContext);
		return inputPlugin.buildContext(taskContext,  record,batchContext);
	}
	@Override
	public String getLastValueVarName() {
//		return importContext.getLastValueColumn();
		return inputPlugin.getLastValueVarName();
	}

	public Long getTimeRangeLastValue(){
		return inputPlugin.getTimeRangeLastValue();
	}
	public DataTranPluginImpl(ImportContext importContext){
		this.importContext = importContext;
		importContext.setDataTranPlugin(this);



//		init(importContext,targetImportContext);


	}

	@Override
	public ImportContext getImportContext() {
		return importContext;
	}

	public void setImportContext(ImportContext importContext) {
		this.importContext = importContext;
	}

    protected ImportContext importContext;


	protected ScheduleService scheduleService;
	protected boolean isPrintTaskLog(){
		return importContext.isPrintTaskLog() && logger.isInfoEnabled();
	}


	public void preCall(TaskContext taskContext){
		List<CallInterceptor> callInterceptors = importContext.getCallInterceptors();
		if(callInterceptors == null || callInterceptors.size() == 0)
			return;
		for(CallInterceptor callInterceptor: callInterceptors){
			try{
				callInterceptor.preCall(taskContext);
			}
			catch (DataImportException e){
				throw new PreCallException(e);
			}
			catch (Exception e){
				throw new PreCallException(e);
			}
		}
		TranUtil.initTaskContextSQLInfo(taskContext, importContext);

	}
	public void afterCall(TaskContext taskContext){
		List<CallInterceptor> callInterceptors = importContext.getCallInterceptors();
		if(callInterceptors == null || callInterceptors.size() == 0)
			return;
		CallInterceptor callInterceptor = null;
		for(int j = callInterceptors.size() - 1; j >= 0; j --){
			callInterceptor = callInterceptors.get(j);
			try{
				callInterceptor.afterCall(taskContext);
			}
			catch (Exception e){
				logger.error("afterCall failed:",e);
				throwException(taskContext, new AfterCallException("afterCall failed:",e));
			}
		}
	}

	public void throwException(TaskContext taskContext,Throwable e){
		List<CallInterceptor> callInterceptors = importContext.getCallInterceptors();
		if(callInterceptors == null || callInterceptors.size() == 0) {
			logger.error("afterCall failed:",e);
			return;
		}
		CallInterceptor callInterceptor = null;
		for(int j = callInterceptors.size() - 1; j >= 0; j --){
			callInterceptor = callInterceptors.get(j);
			try{
				callInterceptor.throwException(taskContext,e);
			}
			catch (Exception e1){
				logger.error("afterCall failed:",e1);
			}
		}

	}

    /**
     * 记录作业处理过程中的debug日志
     *
     * @param taskContext
     * @param msg
     */
    @Override
    public void reportJobMetricDebug(TaskContext taskContext, String msg) {
        if(!MetricsLogLevel.isDebugEnabled(importContext.getMetricsLogLevel())){
            return;
        }
        MetricsLogReport metricsLogReport = importContext.getMetricsLogReport();
        if(metricsLogReport == null ) {
            if(logger.isDebugEnabled())
                logger.debug("reportJobMetricDebug:{}",msg);
            return;
        }
        try{
            if(taskContext == null) {
                taskContext = new TaskContext(importContext,true);

            }
            metricsLogReport.reportJobMetricDebug(taskContext, msg);
        }
        catch (Exception e1){
            logger.error("reportJobMetricDebug failed:"+msg,e1);
        }
    }
 

    /**
     * 记录作业处理过程中的异常日志
     * @param taskContext
     * @param msg
     * @param e
     */
    @Override
    public void reportJobMetricErrorLog(TaskContext taskContext, String msg, Throwable e){
        if(!MetricsLogLevel.isErrorEnabled(importContext.getMetricsLogLevel())){
            return;
        }
        MetricsLogReport metricsLogReport = importContext.getMetricsLogReport();
        if(metricsLogReport == null ) {
            if(logger.isErrorEnabled())
                logger.error("reportMetricErrorLog:"+msg,e);
            return;
        }
        try{
            if(taskContext == null) {
                taskContext = new TaskContext(importContext,true);
                
            }
            metricsLogReport.reportJobMetricErrorLog(taskContext, msg, e);
        }
        catch (Exception e1){
            logger.error("reportMetricErrorLog failed:"+msg,e1);
        }
    }

    /**
     * 记录作业处理过程中的日志
     * @param taskContext
     * @param msg
     */
    @Override
    public void reportJobMetricLog(TaskContext taskContext,String msg){
        if(!MetricsLogLevel.isInfoEnabled(importContext.getMetricsLogLevel())){
            return;
        }
        MetricsLogReport metricsLogReport = importContext.getMetricsLogReport();
        if(metricsLogReport == null ) {
            if(logger.isInfoEnabled())
                logger.info("reportMetricLog:{}",msg);
            return;
        }
        try{
            if(taskContext == null) {
                taskContext = new TaskContext(importContext,true);

            }
            metricsLogReport.reportJobMetricLog(taskContext, msg);
        }
        catch (Exception e1){
            logger.error("reportMetricLog:{}",msg);
        }
    }

    /**
     * 记录作业处理过程中的日志
     * @param taskContext
     * @param msg
     */
    @Override
    public void reportJobMetricWarn(TaskContext taskContext,String msg){
        if(!MetricsLogLevel.isWarnEnabled(importContext.getMetricsLogLevel())){
            return;
        }
        MetricsLogReport metricsLogReport = importContext.getMetricsLogReport();
        if(metricsLogReport == null ) {
            if(logger.isWarnEnabled())
                logger.warn("reportMetricWarn:{}",msg);
            return;
        }
        try{
            if(taskContext == null) {
                taskContext = new TaskContext(importContext,true);

            }
            metricsLogReport.reportJobMetricWarn(taskContext, msg);
        }
        catch (Exception e1){
            logger.error("reportMetricWarn:{}",msg);
        }
    }



    /**
     * 记录作业任务处理过程中的异常日志
     * @param taskMetrics
     * @param msg
     * @param e
     */
     public void reportTaskMetricErrorLog(TaskMetrics taskMetrics, String msg, Throwable e){
         if(!MetricsLogLevel.isErrorEnabled(importContext.getMetricsLogLevel())){
             return;
         }
         MetricsLogReport metricsLogReport = importContext.getMetricsLogReport();
         if(metricsLogReport == null ) {
             logger.error("reportTaskMetricErrorLog:"+msg,e);
             return;
         }
         try{
              
             metricsLogReport.reportTaskMetricErrorLog(taskMetrics, msg,e);
         }
         catch (Exception e1){
             logger.error("reportTaskMetricErrorLog:"+msg,e);
         }
    }



    /**
     * 记录作业任务处理过程中的日志
     * @param taskMetrics
     * @param msg
     */
    public void reportTaskMetricLog(TaskMetrics taskMetrics, String msg){
        if(!MetricsLogLevel.isInfoEnabled(importContext.getMetricsLogLevel())){
            return;
        }
        MetricsLogReport metricsLogReport = importContext.getMetricsLogReport();
        if(metricsLogReport == null ) {
            if(logger.isInfoEnabled())
                logger.info("reportTaskMetricLog:{}",msg);
            return;
        }
        try{

            metricsLogReport.reportTaskMetricLog(taskMetrics, msg);
        }
        catch (Exception e1){
            logger.error("reportTaskMetricLog:"+msg,e1);
        }
    }

    /**
     * 记录作业任务处理过程中的告警日志
     * @param taskMetrics
     * @param msg
     */
    public void reportTaskMetricWarn(TaskMetrics taskMetrics, String msg){
        if(!MetricsLogLevel.isWarnEnabled(importContext.getMetricsLogLevel())){
            return;
        }
        MetricsLogReport metricsLogReport = importContext.getMetricsLogReport();
        if(metricsLogReport == null ) {
            if(logger.isWarnEnabled())
                logger.warn("reportTaskMetricWarn:{}",msg);
            return;
        }
        try{

            metricsLogReport.reportTaskMetricWarn(taskMetrics, msg);
        }
        catch (Exception e1){
            logger.error("reportTaskMetricWarn:"+msg,e1);
        }
    }


    /**
     * 记录作业任务处理过程中的debug日志
     * @param taskMetrics
     * @param msg
     */
    public void reportTaskMetricDebug(TaskMetrics taskMetrics, String msg){
        if(!MetricsLogLevel.isDebugEnabled(importContext.getMetricsLogLevel())){
            return;
        }
        MetricsLogReport metricsLogReport = importContext.getMetricsLogReport();
        if(metricsLogReport == null ) {
            if(logger.isDebugEnabled())
                logger.debug("reportTaskMetricDebug:{}",msg);
            return;
        }
        try{

            metricsLogReport.reportTaskMetricDebug(taskMetrics, msg);
        }
        catch (Exception e1){
            logger.error("reportTaskMetricDebug:"+msg,e1);
        }
    }
	@Override
	public boolean isEnableAutoPauseScheduled(){
		return true;
	}


	protected Thread delayThread ;
	protected Thread scheduledEndThread;
	protected void delay(){
		Long deyLay = importContext.getDeyLay();
		Date date = importContext.getScheduleDate();
		long _delay = 0l;
		if(date != null){
			_delay = date.getTime() - System.currentTimeMillis();
		}
		else if(deyLay != null && deyLay > 0l){
			_delay = deyLay;
		}
		final long tmp = _delay;
		if(tmp >  0) {
			delayThread = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						sleep(tmp);
					} catch (InterruptedException e) {
						logger.info("job delay is interrupted.");
					}
				}
			},"Datatran-DelayThread");
			delayThread.start();
			try {
				delayThread.join();//等待线程执行完毕
			} catch (InterruptedException e) {
				logger.info("job delay join is interrupted.");
			}
			delayThread = null;
		}
	}
    
    private Object startEndScheduleThreadLock = new Object();

    /**
     * 启动作业自动结束线程
     * @param scheduled
     * @param scheduleEndCall
     */
    protected void startEndScheduleThread(boolean scheduled, ScheduleEndCall scheduleEndCall){
        Date scheduleEndDate = importContext.getScheduleEndDate();
        if(scheduled && scheduleEndDate != null){

            if(importContext.isExecuteOneTime()){
                logger.info("一次性执行作业，忽略Start EndSchedule Thread：scheduleEndDate[{}]", DateFormatUtils.format(scheduleEndDate,"yyyy-MM-dd HH:mm:ss.SSS"));
                return;
            }
            synchronized (startEndScheduleThreadLock) {
                if(scheduledEndThread == null) {
                    final long waitTime = scheduleEndDate.getTime() - System.currentTimeMillis();

                    scheduledEndThread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            if (waitTime > 0) {
                                try {
                                    sleep(waitTime);
                                    scheduleEndCall.call(true);
                                } catch (InterruptedException e) {

                                }
                            } else {
                                scheduleEndCall.call(true);
                            }

                        }
                    }, "Datatran-ScheduledEndThread");
                    scheduledEndThread.setDaemon(true);
                    scheduledEndThread.start();
                }
            }
        }
    }
	@Override
	public void importData(ScheduleEndCall scheduleEndCall) throws DataImportException {
        if(this.checkTranToStop())//任务处于停止状态，不再执行定时作业
        {
            return;
        }
		if(this.scheduleService == null || importContext.isExecuteOneTime()) {//一次性执行数据导入操作
			delay();//针对一次性作业进行延迟处理
//			if(status == TranConstant.PLUGIN_STOPPED || status == TranConstant.PLUGIN_STOPAPPENDING)
            if(checkTranToStop())
				return;
			long importStartTime = System.currentTimeMillis();

			TaskContext taskContext = inputPlugin.isEnablePluginTaskIntercept()?new TaskContext(importContext):null;
            Exception exception = null;
			try {
				if(inputPlugin.isEnablePluginTaskIntercept())
					preCall(taskContext);
//				this.doImportData(taskContext);
				this.inputPlugin.doImportData(taskContext);
				if(inputPlugin.isEnablePluginTaskIntercept())
					afterCall(taskContext);
				long importEndTime = System.currentTimeMillis();
				if( isPrintTaskLog())
					logger.info(new StringBuilder().append("Execute job Take ").append((importEndTime - importStartTime)).append(" ms").toString());
			}
			catch (Exception e){
				if(inputPlugin.isEnablePluginTaskIntercept())
					throwException(taskContext,e);
				logger.error("scheduleImportData failed:",e);
                exception = e;

			}
            finally {
                if(!importContext.getDataTranPlugin().isMultiTran())
                    importContext.finishAndWaitTran(exception);
            }

		}
		else{//定时增量导入数据操作
			try {
				if (!this.importContext.isExternalTimer()) {//内部定时任务引擎
					Date scheduleEndDate = importContext.getScheduleEndDate();
					Date now = new Date();
					if(scheduleEndDate != null) {
						if (now.after(scheduleEndDate)) {
							logger.info("Job scheduleEndDate reached,Ignore schedule this job.");
							return;
						}

					}
					boolean scheduled = scheduleService.timeSchedule(   );
                    startEndScheduleThread(   scheduled,  scheduleEndCall);
				} else { //外部定时任务引擎执行的方法，比如quartz，xxl-job之类的
                    startEndScheduleThread(  true,  scheduleEndCall);
					if(scheduleService.isSchedulePaused(isEnableAutoPauseScheduled())){ //如果作业处于暂停状态，回阻塞等待，直到consume使作业恢复执行，并返回true
						
                        if(this.checkTranToStop())//任务处于停止状态，不再执行定时作业
                        {
                            if(logger.isInfoEnabled()){
                                logger.info("Schedule Task has stopped,stop resume schedule.");
                            } 
                            return;
                        }
//						return;
					}
					scheduleService.externalTimeSchedule();

				}
			}
			catch (DataImportException e)
			{
				throw e;
			}
			catch (Exception e)
			{
				throw ImportExceptionUtil.buildDataImportException(importContext,e);
			}
		}

	}




	public  void beforeInit(){
//		initOtherDSes(importContext.getConfigs());
		this.inputPlugin.beforeInit();
		this.outputPlugin.beforeInit();
	}
//	public abstract void afterInit();
//	public abstract void initStatusTableId();
	public void initStatusTableId(){
		inputPlugin.initStatusTableId();
	}
	@Override
	public void loadCurrentStatus(List<Status> statuses){

	}

	@Override
	public String getJobType() {
		return inputPlugin.getJobType();
	}

	@Override
	public LoadCurrentStatus getLoadCurrentStatus(){
		return statusManager.getLoadCurrentStatus();
	}
	protected void initStatusManager(){
		statusManager = new SingleStatusManager(this);
//		statusManager.init();
	}

	private void _initStatusManager(){
		if(this.importContext.isAsynFlushStatus()) {
			initStatusManager();
		}
		else{
			statusManager = new DefaultStatusManager(this);
//			statusManager.init();
		}
		statusManager.initTableAndStatus(getInitLastValueClumnName());
	}
	protected InitLastValueClumnName getInitLastValueClumnName(){
		return new InitLastValueClumnName (){

			public void initLastValueClumnName(){
				statusManager.initLastValueClumnName();
			}
		};
	}
	@Override
	public SetLastValueType getSetLastValueType(){
		return new SetLastValueType (){

			public void set(){
				statusManager.initLastValueType();
			}
		};
	}
	protected boolean initOtherDSes ;
	protected boolean initDefaultDS;
	public void initDefaultDS(){
		if(initDefaultDS )
			return;
		try {
			DBConfig dbConfig = importContext.getDefaultDBConfig();
			if (dbConfig != null ) {
				initDS(dbStartResult,dbConfig);
			}
		}
		finally {
			initDefaultDS = true;
		}
	}
	public void initOtherDSes(){
		if(initOtherDSes )
			return;
		try {
			List<DBConfig> dbConfigs = importContext.getOhterDBConfigs();
			if (dbConfigs != null && dbConfigs.size() > 0) {
				for (DBConfig dbConfig : dbConfigs) {
					initDS(dbStartResult,dbConfig);
				}
			}
		}
		finally {
			initOtherDSes = true;
		}
	}

    
	public static void initDS(DBStartResult dbStartResult,DBConfig dbConfig){
        
        if(dbConfig != null){
            if(SimpleStringUtil.isEmpty(dbConfig.getDbName())){
                throw new DataImportException("dbname can not be empty in DBConfig "+SimpleStringUtil.object2json(dbConfig));
            }
            if(!dbStartResult.contain(dbConfig.getDbName())){
                if(SimpleStringUtil.isNotEmpty(dbConfig.getDbDriver()) && SimpleStringUtil.isEmpty(dbConfig.getDbUrl()))
                    throw new DataImportException("DbUrl can not be empty in DBConfig "+SimpleStringUtil.object2json(dbConfig));

                if(SimpleStringUtil.isEmpty(dbConfig.getDbDriver()) && SimpleStringUtil.isNotEmpty(dbConfig.getDbUrl()))
                    throw new DataImportException("DbDriver can not be empty in DBConfig "+SimpleStringUtil.object2json(dbConfig));
            }
        }
		if(dbConfig != null && SimpleStringUtil.isNotEmpty(dbConfig.getDbName())
				&& SimpleStringUtil.isNotEmpty(dbConfig.getDbDriver())
				&& SimpleStringUtil.isNotEmpty(dbConfig.getDbUrl()) && !dbStartResult.contain(dbConfig.getDbName())) {
			DBConf temConf = new DBConf();
			temConf.setPoolname(dbConfig.getDbName());
			temConf.setDriver(dbConfig.getDbDriver());
			temConf.setJdbcurl(dbConfig.getDbUrl());
			temConf.setUsername(dbConfig.getDbUser());
			temConf.setPassword(dbConfig.getDbPassword());
			temConf.setReadOnly(null);
			temConf.setTxIsolationLevel(null);
			temConf.setValidationQuery(dbConfig.getValidateSQL());
			temConf.setJndiName(dbConfig.getDbName()+"_jndi");
			temConf.setInitialConnections(dbConfig.getInitSize());
			temConf.setMinimumSize(dbConfig.getMinIdleSize());
			temConf.setMaximumSize(dbConfig.getMaxSize());
			temConf.setUsepool(dbConfig.isUsePool());
			temConf.setExternal(false);
			temConf.setExternaljndiName(null);
			temConf.setShowsql(dbConfig.isShowSql());
			temConf.setEncryptdbinfo(false);
			temConf.setQueryfetchsize(dbConfig.getJdbcFetchSize() == null?null:dbConfig.getJdbcFetchSize());
			temConf.setDbAdaptor(dbConfig.getDbAdaptor());
			temConf.setDbtype(dbConfig.getDbtype());
			temConf.setColumnLableUpperCase(dbConfig.isColumnLableUpperCase());
			temConf.setDbInfoEncryptClass(dbConfig.getDbInfoEncryptClass());
            temConf.setConnectionTimeout(dbConfig.getConnectionTimeout());
            temConf.setMaxIdleTime(dbConfig.getMaxIdleTime());
            temConf.setMaxWait(dbConfig.getMaxWait());
            temConf.setConnectionProperties(dbConfig.getConnectionProperties());
            temConf.setEnableBalance(dbConfig.isEnableBalance());
            temConf.setBalance(dbConfig.getBalance());
            temConf.setDatasource(dbConfig.getDataSource());
            boolean ret = SQLManager.startPool(temConf);
			if(ret){
				dbStartResult.addDBStartResult(temConf.getPoolname());
			}

		}
        else if(dbConfig.getDataSource() != null){
            DBConf temConf = new DBConf();
            temConf.setPoolname(dbConfig.getDbName());
            temConf.setJndiName(dbConfig.getDbName()+"_jndi");
            temConf.setDriver(dbConfig.getDbDriver());
            temConf.setShowsql(dbConfig.isShowSql());            
            temConf.setQueryfetchsize(dbConfig.getJdbcFetchSize() == null?null:dbConfig.getJdbcFetchSize());
            temConf.setDbAdaptor(dbConfig.getDbAdaptor());
            temConf.setDbtype(dbConfig.getDbtype());
            temConf.setColumnLableUpperCase(dbConfig.isColumnLableUpperCase());            
            temConf.setDatasource(dbConfig.getDataSource());
            boolean ret = SQLManager.startPool(temConf);
            if(ret){
                dbStartResult.addDBStartResult(temConf.getPoolname());
            }
        }
	}

	@Override
	public void init(ImportContext importContext) {
        
		this.importContext = importContext;
        this.importContext.initJobcontext();
        this.importContext.initETLMetrics();
		exportCount = new ExportCount();
		this.inputPlugin = importContext.getInputPlugin();
		this.outputPlugin = importContext.getOutputPlugin();
		inputPlugin.setDataTranPlugin(this);
		outputPlugin.setDataTranPlugin(this);
		initDefaultDS();
		initOtherDSes();
		beforeInit();
		this.inputPlugin.init();
		this.outputPlugin.init();

		this.initSchedule();
		_initStatusManager();
		inputPlugin.afterInit();
		outputPlugin.afterInit();
	}
	public boolean isMultiTran(){
		return inputPlugin.isMultiTran();
	}
	public String getLastValueClumnName(){
		return statusManager.getLastValueClumnName();
	}
	public boolean isContinueOnError(){
		return this.importContext.isContinueOnError();
	}

	@Override
	public Status getCurrentStatus() {
		return statusManager.getCurrentStatus();
	}

	/**
	 * 插件运行状态
	 */
	protected volatile int status = TranConstant.PLUGIN_INIT;
    protected volatile CountDownLatch latch = null;
	protected volatile boolean hasTran = false;
    protected ReentrantLock lock = new ReentrantLock();
	/**
	 *
	 */
	private AtomicInteger tranCounts = new AtomicInteger(0);
    protected void initLatch(){
        synchronized (latchLock) {
            if(latch == null || latch.getCount() == 0) {
                latch = new CountDownLatch(1);
            }
        }
    }
	public void setHasTran(){
		lock.lock();
		try {
            initLatch();
			tranCounts.incrementAndGet();
			this.hasTran = true;
            if(status == TranConstant.PLUGIN_STOPREADY || status == TranConstant.PLUGIN_INIT)
			    status = TranConstant.PLUGIN_TRAN_START;
		}
		finally {
			lock.unlock();
		}

	}
    protected void latchCountDown(){
        synchronized (latchLock) {
            if (latch != null)
                latch.countDown();
        }
    }
    public void checkHasTranAndSetPLUGIN_STOPREADY() {
        lock.lock();
        try {
            if(!hasTran) {
                if(status != TranConstant.PLUGIN_STOPAPPENDING)
                    this.status = TranConstant.PLUGIN_STOPREADY;
                else
                    this.status = TranConstant.PLUGIN_STOPAPPENDING_STOPREADY;
                latchCountDown();
            }
        }
        finally {
            lock.unlock();
        }
    }


    public boolean isHasTran() {
        lock.lock();
        try {
            return hasTran;
        }
        finally {
            lock.unlock();
        }
    }

    public static void stopDatasources(DBStartResult dbStartResult){
		if(dbStartResult != null ){
			Map<String,Object> dbs = dbStartResult.getDbstartResult();
			if(dbs != null && dbs.size() > 0){
				Iterator<Map.Entry<String, Object>> iterator = dbs.entrySet().iterator();
				while(iterator.hasNext()){
					Map.Entry<String, Object> entry = iterator.next();

					String db = entry.getKey();
					try {
						SQLUtil.stopPool(db);
					} catch (Exception e) {
						if(logger.isErrorEnabled())
							logger.error("SQLUtil.stopPool("+db+") failed:",e);
					}
				}
			}
		}
	}
	public void setNoTran(){
        _setNoTran(true);
	}

    protected void _setNoTran(boolean modifyStatus){
        lock.lock();
        try {

            int count = tranCounts.decrementAndGet();
            if(count <= 0) {
                this.hasTran = false;
                if(modifyStatus) {
                    if (status != TranConstant.PLUGIN_STOPAPPENDING)
                        this.status = TranConstant.PLUGIN_STOPREADY;
                    else
                        this.status = TranConstant.PLUGIN_STOPAPPENDING_STOPREADY;
                    latchCountDown();
                }
            }
        }
        finally {
            lock.unlock();
        }
    }
	public boolean isStopCollectData(){
		return inputPlugin.isStopCollectData();
	}

    @Override
    public boolean isPluginStopREADY() {
        return  this.status == TranConstant.PLUGIN_STOPREADY ||  this.status == TranConstant.PLUGIN_STOPAPPENDING_STOPREADY ;
    }



    @Override
    public boolean isPluginStopAppending(){
		lock.lock();
		try {
			return status == TranConstant.PLUGIN_STOPAPPENDING || status == TranConstant.PLUGIN_STOPAPPENDING_STOPREADY;
		}
		finally {
			lock.unlock();
		}
	}


    /**
     * AsynBaseTranResultSet用于判断是否结束当前遍历结果数据，
     * FileReaderTask用于判断是否结束当前文件记录采集任务
     * 根据外部指令设置的
     * @return
     */
	public boolean checkTranToStop(){
		lock.lock();
		try {

			return status == TranConstant.PLUGIN_STOPAPPENDING  || status == TranConstant.PLUGIN_STOPAPPENDING_STOPREADY
				|| status == TranConstant.PLUGIN_STOPPED ;

            //TranConstant.PLUGIN_STOPREADY是所有tran执行完成后自动设置的状态标记，说明次状态可以退出同步作业
//            return status == TranConstant.PLUGIN_STOPAPPENDING
//                    || status == TranConstant.PLUGIN_STOPREADY || status == TranConstant.PLUGIN_STOPPED ;
		}
		finally {
			lock.unlock();
		}
	}
    protected Object latchLock = new Object();
    protected void canFinishTran(boolean onceTaskFinish){
//        lock.lock();
//        try{
////            if(!onceTaskFinish) //如果是一次性任务结束，不需要检查TranConstant.PLUGIN_INIT状态，如果是通过destroy结束任务，则需要判断TranConstant.PLUGIN_INIT
//            if( status == TranConstant.PLUGIN_INIT )
//                return ;
////            else{
////                return status == TranConstant.PLUGIN_STOPREADY  || status == TranConstant.PLUGIN_STOPAPPENDING_STOPREADY;
////            }
//        }
//        finally {
//            lock.unlock();
//        }

        try {
            CountDownLatch countDownLatch = null;
            synchronized (latchLock){
                countDownLatch = this.latch;
            }
            if (countDownLatch != null) {
                countDownLatch.await();
            }
        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
        }
//        lock.lock();
//        try{
////            if(!onceTaskFinish) //如果是一次性任务结束，不需要检查TranConstant.PLUGIN_INIT状态，如果是通过destroy结束任务，则需要判断TranConstant.PLUGIN_INIT
//                return status == TranConstant.PLUGIN_INIT || status == TranConstant.PLUGIN_STOPREADY  || status == TranConstant.PLUGIN_STOPAPPENDING_STOPREADY;
////            else{
////                return status == TranConstant.PLUGIN_STOPREADY  || status == TranConstant.PLUGIN_STOPAPPENDING_STOPREADY;
////            }
//        }
//        finally {
//            lock.unlock();
//        }
    }

	protected void checkTranFinished(boolean onceTaskFinished){
        canFinishTran( onceTaskFinished);
//		do {
//			if (canFinishTran( onceTaskFinished)) {
//				break;
//			}
//			try {
//				sleep(1000l);
//			} catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                
//			}
//		} while (true);
	}
    protected void destroyExportResultHandler(){
        importContext.getOutputConfig().destroyExportResultHandler();
    }
	protected void _afterDestory(boolean onceTaskFinished,boolean waitTranStop,boolean fromScheduleEnd,Throwable throwable){
		checkTranFinished(onceTaskFinished);

//		WrapedExportResultHandler wrapedExportResultHandler = importContext.getExportResultHandler();
//		if(wrapedExportResultHandler != null){
//			try {
//				wrapedExportResultHandler.destroy();
//			}
//			catch (Throwable e){
//				logger.warn("Destroy WrapedExportResultHandler failed:",e);
//			}
//		}
        destroyExportResultHandler();
        stopMetrics();
		if(statusManager != null)
			statusManager.stop();
		//释放资源开始
        endAction();
		inputPlugin.destroy(waitTranStop);
		outputPlugin.destroy(waitTranStop);
		statusManager.stopStatusDatasource();
		stopDatasources(dbStartResult);
		//释放资源结束
		status = TranConstant.PLUGIN_STOPPED;
		importContext.cleanResource();
        List<JobClosedListener> jobClosedListeners = importContext.getJobClosedListeners();
        if(jobClosedListeners != null && jobClosedListeners.size() > 0){
            for(JobClosedListener jobClosedListener:jobClosedListeners){
                try {
                    jobClosedListener.jobClosed(importContext, throwable);
                }
                catch (Exception e){
                    logger.warn("Execute jobclose callback failed: ",e);
                }
            }
//            importContext.getJobClosedListeners().jobClosed(importContext,throwable);
        }
	}
    private void endAction(){
        if(importContext.getEndAction() != null) {
            try {
                importContext.getEndAction().endAction();
            }
            catch (Exception e){
                logger.error("importContext.getEndAction().endAction failed:",e);
            }
        }
    }
    private void stopMetrics(){
        List<ETLMetrics> etlMetrics = importContext.getMetrics();
        if(etlMetrics != null && etlMetrics.size() > 0){
            for(ETLMetrics etlMetric : etlMetrics){
                //强制刷指标数据
                try {
                    etlMetric.stopMetrics();
                }
                catch (Exception e){
                    logger.error("",e);
                }

            }
        }
        

        etlMetrics = importContext.getOutputConfig().getMetrics();
        if(etlMetrics != null && etlMetrics.size() > 0){
            for(ETLMetrics etlMetric : etlMetrics){
                //强制刷指标数据
                try {
                    etlMetric.stopMetrics();
                }
                catch (Exception e){
                    logger.error("",e);
                }

            }
        }
    }
    @Override
    public void finishAndWaitTran(Throwable throwable){
        if(checkTranToStop()){
            return;
        }
        logger.info("Finish datatran job begin.");
        if(scheduleService != null){
            scheduleService.stop();
        }
        if(throwable != null) {
            try {
                inputPlugin.stopCollectData();
            } catch (Exception e) {
                logger.warn("", e);
            }

            try {
                outputPlugin.stopCollectData();
            } catch (Exception e) {
                logger.warn("", e);
            }
        }

        if(delayThread != null){
            try {
                delayThread.interrupt();
                delayThread.join();

            }
            catch (Exception e){

            }
        }
        if (scheduledEndThread != null) {
            try {
                scheduledEndThread.interrupt();
                scheduledEndThread.join();
            } catch (Exception e) {

            }
            scheduledEndThread = null;
        }

        _afterDestory(true, true, false,throwable);
        logger.info("Finish datatran job completed.");

    }
    protected void PLUGIN_STOPAPPENDING(){
        lock.lock();
        try {
            if(this.status != TranConstant.PLUGIN_STOPREADY) {
                if(this.hasTran) {
                    this.status = TranConstant.PLUGIN_STOPAPPENDING;
                }
                else{
                    this.status = TranConstant.PLUGIN_STOPAPPENDING_STOPREADY;
                    latchCountDown();
                }
            }
            else {
                this.status = TranConstant.PLUGIN_STOPAPPENDING_STOPREADY;
                latchCountDown();
            }
        }
        finally {
            lock.unlock();
        }
    }

    private DestroyPolicy destroyPolicy;

    public DestroyPolicy getDestroyPolicy() {
        return destroyPolicy;
    }

    @Override
	public void destroy(DestroyPolicy destroyPolicy) {
        if(checkTranToStop()){
            return;
        }
        this.destroyPolicy = destroyPolicy;
        boolean waitTranStop = destroyPolicy.isWaitTranStopped();
        boolean fromScheduleEnd = destroyPolicy.isFromScheduleEnd();
        if(logger.isInfoEnabled())
            logger.info("Destroy datatran job begin with waitTranStop {} fromScheduleEnd {}",waitTranStop,fromScheduleEnd);
        PLUGIN_STOPAPPENDING();
        if(scheduleAssert != null){
            this.scheduleAssert.stopAndInteruptPause();
        }
		if(scheduleService != null){
			scheduleService.stop();
		}
		try {
			inputPlugin.stopCollectData();
		}
		catch (Exception e){
			logger.warn("",e);
		}

		try {
			outputPlugin.stopCollectData();
		}
		catch (Exception e){
			logger.warn("",e);
		}
		if(delayThread != null){
			try {
				delayThread.interrupt();
                delayThread.join();

			}
			catch (Exception e){

			}
		}
		if(!fromScheduleEnd) {
			if (scheduledEndThread != null) {
				try {
					scheduledEndThread.interrupt();
                    scheduledEndThread.join();
				} catch (Exception e) {

				}
				scheduledEndThread = null;
			}
		}

		if(waitTranStop) {
			_afterDestory( false,waitTranStop, fromScheduleEnd,(Throwable)null);
            if(logger.isInfoEnabled())
                logger.info("Destroy datatran job complete with waitTranStop {} fromScheduleEnd {}",waitTranStop,fromScheduleEnd);

		}
		else{
			Thread stopThread = new Thread(new Runnable() {
				@Override
				public void run() {
					_afterDestory(false, waitTranStop, fromScheduleEnd,(Throwable)null);
                    if(logger.isInfoEnabled())
                        logger.info("Destroy datatran job complete with waitTranStop {} fromScheduleEnd {}",waitTranStop,fromScheduleEnd);
				}
			},"Destroy-DataTranPlugin-Thread");
			stopThread.start();

		}


	}



	@Override
	public Object[] putLastParamValue(Map params){

		return statusManager.putLastParamValue(params);
	}

	@Override
	public boolean isIncreamentImport() {
		return statusManager.isIncreamentImport();
	}


	public Map getParamValue(Map params){

		return statusManager.getParamValue(params);
	}





	public int getLastValueType() {
		return statusManager.getLastValueType();
	}

    public boolean isSingleLastValueType(){
        return true;
    }




    @Override
    public void flushLastValue(LastValueWrapper lastValueWrapper,Status currentStatus,boolean reachEOFClosed){
//        Long timeLastValue = this.getTimeRangeLastValue();
//        if(timeLastValue != null){
//
//            Object lastValue = max(lastValueWrapper.getLastValue(),new Date(timeLastValue));
//            lastValueWrapper.setLastValue(lastValue);
//        }
//        this.flushLastValue(lastValueWrapper,currentStatus,  reachEOFClosed);
        statusManager.flushLastValue(lastValueWrapper,  currentStatus,  reachEOFClosed);
    }
    @Override
    public boolean needUpdateLastValueWrapper(Integer lastValueType, LastValueWrapper oldValue,LastValueWrapper newValue){
        return BaseStatusManager.needUpdate(lastValueType, oldValue.getLastValue(),newValue.getLastValue());
    }
//	@Override
//	public void flushLastValue(LastValueWrapper lastValue,Status currentStatus) {
//		statusManager.flushLastValue(lastValue,  currentStatus);
//	}

//	@Override
//	public void forceflushLastValue(Status currentStatus) {
//		statusManager.forceflushLastValue(   currentStatus);
//
//	}



	@Override
	public void handleOldedTasks(List<Status> olded) {
		statusManager.handleOldedTasks(olded);
	}

	@Override
	public void handleOldedTask(Status olded) {
		statusManager.handleOldedTask(olded);
	}




	public ScheduleService getScheduleService(){
		return this.scheduleService;
	}




	//	private String indexType;
	private TranErrorWrapper errorWrapper;

	public TranErrorWrapper getErrorWrapper() {
		return errorWrapper;
	}

	public void setErrorWrapper(TranErrorWrapper errorWrapper) {
		this.errorWrapper = errorWrapper;
	}


	private volatile boolean forceStop = false;
	public void setForceStop(){
		this.forceStop = true;
	}
	/**
	 * 判断执行条件是否成立，成立返回true，否则返回false
	 * @return
	 */
	public boolean assertCondition(){
		if(forceStop)
			return false;
		if(errorWrapper != null)
			return errorWrapper.assertCondition();
		return true;
	}

	/**
	 * 判断执行条件是否成立，成立返回true，否则返回false
	 * @return
	 */
	public boolean assertCondition(Exception e){
		if(errorWrapper != null)
			return errorWrapper.assertCondition(e);
		return true;
	}


	public void initSchedule(){
		if(importContext.getScheduleConfig() != null) {
			this.scheduleService = new ScheduleService();
			scheduleService.setEnablePluginTaskIntercept(inputPlugin.isEnablePluginTaskIntercept());
			this.scheduleService.init(importContext);
		}
	}
	private List<ResourceStartResult> resourceStartResults;
	@Override
	public void initResources(ResourceStart resourceStart) {

		try {
			ResourceStartResult resourceStartResult = resourceStart.startResource();

			if(resourceStartResult != null) {
				if(resourceStartResults == null){
					resourceStartResults = new ArrayList<>();
				}
				resourceStartResults.add(resourceStartResult);
			}
		}
        catch (DataImportException e){
            throw e;
        }
		catch (Exception e){
//			logger.error("Resource Start failed:",e);
            throw new DataImportException("Resource Start failed:",e);
		}

	}

	@Override
	public void destroyResources(ResourceEnd resourceEnd) {
		if(resourceStartResults != null){
			for(ResourceStartResult resourceStartResult: resourceStartResults){
				try {
					resourceEnd.endResource(resourceStartResult);
				}
				catch (Exception e){
					logger.error("End Resource failed:",e);
				}
			}
		}
	}

    @Override
    public void initLastValueStatus(Status currentStatus,BaseStatusManager baseStatusManager) throws Exception {

        LastValueWrapper lastValueWrapper = currentStatus.getCurrentLastValueWrapper();

        if(importContext.isLastValueDateType()) {
            Object configLastValue = importContext.getConfigLastValue();
            if(configLastValue != null){

                if(configLastValue instanceof Date) {
                    lastValueWrapper.setLastValue(configLastValue);

                }
                else if(configLastValue instanceof Long){
                    lastValueWrapper.setLastValue(new Date((Long)configLastValue));
                }
                else if(configLastValue instanceof BigDecimal){
                    lastValueWrapper.setLastValue(new Date(((BigDecimal)configLastValue).longValue()));
                }

                else if(configLastValue instanceof Integer){
                    lastValueWrapper.setLastValue(new Date((Integer)configLastValue));
                }
                else{
                    if(logger.isInfoEnabled()) {
                        logger.info("TIMESTAMP TYPE Last Value Illegal:{}", configLastValue);
                    }
                    throw ImportExceptionUtil.buildDataImportException(importContext,"TIMESTAMP TYPE Last Value Illegal:"+configLastValue );
                }
//                lastValueWrapper.setLastValue(currentStatus.getLastValue());
            }
            else {
//				currentStatus.setLastValue(initLastDate);
                lastValueWrapper.setLastValue(baseStatusManager.getInitLastDate());
            }
        }
        else if(importContext.isLastValueNumberType()) {
            if (importContext.getConfigLastValue() != null) {

                lastValueWrapper.setLastValue(importContext.getConfigLastValue());
            } else {
                lastValueWrapper.setLastValue(0l);
            }
//            lastValueWrapper.setLastValue(currentStatus.getLastValue());
        }
        else if(importContext.isLastValueLocalDateTimeType()) {
            Object configLastValue = importContext.getConfigLastValue();
            if(configLastValue != null){

                if(configLastValue instanceof String) {
                    LocalDateTime localDateTime = TimeUtil.localDateTime((String) configLastValue);
                    lastValueWrapper.setLastValue(localDateTime);
                    lastValueWrapper.setStrLastValue((String) configLastValue);
                }
                else  if(configLastValue instanceof LocalDateTime){
                    lastValueWrapper.setLastValue(configLastValue);
                    lastValueWrapper.setStrLastValue(TimeUtil.changeLocalDateTime2String( (LocalDateTime)configLastValue,importContext.getLastValueDateformat()));
                }

                else{
                    if(logger.isInfoEnabled()) {
                        logger.info("TIMESTAMP TYPE Last Value Illegal:{}", configLastValue);
                    }
                    throw ImportExceptionUtil.buildDataImportException(importContext,"TIMESTAMP TYPE Last Value Illegal:"+configLastValue );
                }

            }
            else {
                lastValueWrapper.setLastValue(baseStatusManager.getInitLastLocalDateTime());
//                currentStatus.setStrLastValue(TimeUtil.changeLocalDateTime2String( initLastLocalDateTime,importContext.getLastValueDateformat()));
            }
//            lastValueWrapper.setLastValue(currentStatus.getLastValue());
//            lastValueWrapper.setLastValue(currentStatus.getStrLastValue());
        }
    }

}
