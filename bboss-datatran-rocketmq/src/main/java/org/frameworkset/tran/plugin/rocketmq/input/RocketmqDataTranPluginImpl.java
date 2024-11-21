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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPluginImpl;
import org.frameworkset.tran.DestroyPolicy;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ScheduleEndCall;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.InitLastValueClumnName;
import org.frameworkset.tran.util.EventListenStoppedThread;
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
public class RocketmqDataTranPluginImpl extends DataTranPluginImpl {

	protected static Logger logger = LoggerFactory.getLogger(RocketmqDataTranPluginImpl.class);

	private StoppedThread metricsThread;

	/**
	 * 默认十分钟执行一次拦截器监控数据afterCall方法
	 */
	private long metricsInterval ;
	public RocketmqDataTranPluginImpl(ImportContext importContext){
		super(importContext);
		metricsInterval = ((RocketmqInputConfig)importContext.getInputConfig()).getMetricsInterval();
		if(metricsInterval <= 0L){
			metricsInterval = DEFAULT_metricsInterval;
		}
	}

	@Override
	public void beforeInit() {
		super.beforeInit();


	}



	@Override
	public void importData(ScheduleEndCall scheduleEndCall) throws DataImportException {


		long importStartTime = System.currentTimeMillis();
		TaskContext taskContext = inputPlugin.isEnablePluginTaskIntercept()?new TaskContext(importContext):null;
		try {
			preCall(taskContext);
			this.doImportData(taskContext);
			List<CallInterceptor> callInterceptors = importContext.getCallInterceptors();
			if(callInterceptors != null && callInterceptors.size() > 0) {
				metricsThread = new EventListenStoppedThread(taskContext,this,metricsInterval);
				metricsThread.setName("RocketmqDataTranPlugin-MetricsThread");
				metricsThread.setDaemon(true);
				metricsThread.start();
			}

			long importEndTime = System.currentTimeMillis();
			if (isPrintTaskLog())
				logger.info(new StringBuilder().append("Execute job Take ").append((importEndTime - importStartTime)).append(" ms").toString());
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
			DataImportException e = ImportExceptionUtil.buildDataImportException(importContext,dataImportException);
			throwException(taskContext,e);
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
	protected InitLastValueClumnName getInitLastValueClumnName(){
		return new InitLastValueClumnName (){

			public void initLastValueClumnName(){
				statusManager.setIncreamentImport(false);
			}
		};
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
}
