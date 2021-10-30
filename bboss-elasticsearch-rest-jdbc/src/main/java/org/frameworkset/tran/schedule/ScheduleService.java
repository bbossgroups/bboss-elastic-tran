package org.frameworkset.tran.schedule;
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.timer.ScheduleTimer;
import org.frameworkset.tran.schedule.timer.TimerScheduleConfig;
import org.frameworkset.tran.util.TranUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/8 17:31
 * @author biaoping.yin
 * @version 1.0
 */
public class ScheduleService {
	private static Logger logger = LoggerFactory.getLogger(ScheduleService.class);

	protected boolean enablePluginTaskIntercept = true;

	public void setEnablePluginTaskIntercept(boolean enablePluginTaskIntercept) {
		this.enablePluginTaskIntercept = enablePluginTaskIntercept;
	}

	public boolean isEnablePluginTaskIntercept() {
		return enablePluginTaskIntercept;
	}


	/**
	 * 采用外部定时任务引擎执行定时任务控制变量：
	 * false 内部引擎，默认值
	 * true 外部引擎
	 */

	private ImportContext importContext;

	private ImportContext targetImportContext;



	private Timer timer ;
	private ScheduleTimer scheduleTimer;

	private void scheduleImportData(TaskContext taskContext) throws Exception {
		if(!importContext.assertCondition()) {
			if(logger.isWarnEnabled())
				logger.warn(new StringBuilder().append("Task Assert Execute Condition Failed, Ignore").toString());
			return;
		}
		importContext.doImportData(  taskContext);
//		SQLInfo sqlInfo = getLastValueSQL();


	}
	private void preCall(TaskContext taskContext){
		List<CallInterceptor> callInterceptors = importContext.getCallInterceptors();
		if(callInterceptors == null || callInterceptors.size() == 0)
			return;
		for(CallInterceptor callInterceptor: callInterceptors){
			try{
				callInterceptor.preCall(taskContext);
			}
			catch (Exception e){
				logger.error("preCall failed:",e);
			}
		}
		TranUtil.initTaskContextSQLInfo(taskContext, importContext,
				targetImportContext);

	}
	private void afterCall(TaskContext taskContext){
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
			}
		}
	}

	private void throwException(TaskContext taskContext,Exception e){
		List<CallInterceptor> callInterceptors = importContext.getCallInterceptors();
		if(callInterceptors == null || callInterceptors.size() == 0)
			return;
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
	private void jdkTimeSchedule(ScheduleConfig scheduleConfig ) throws Exception{
		//		scheduleImportData(dataTranPlugin.getBatchSize());

		timer = new Timer();
		TimerTask timerTask = new TimerTask() {
			@Override
			public void run() {
//				TaskContext taskContext = new TaskContext(dataTranPlugin);
//				try {
//					preCall(taskContext);
//					scheduleImportData(dataTranPlugin.getScheduleBatchSize());
//					afterCall(taskContext);
//				}
//				catch (Exception e){
//					throwException(taskContext,e);
//					logger.error("scheduleImportData failed:",e);
//				}
				externalTimeSchedule( );
			}
		};
		Date scheduleDate = scheduleConfig.getScheduleDate();
		Long delay = scheduleConfig.getDeyLay();
		if(scheduleDate != null) {
			if (scheduleConfig.getFixedRate() != null && scheduleConfig.getFixedRate()) {

				timer.scheduleAtFixedRate(timerTask, scheduleDate, scheduleConfig.getPeriod());
			} else {
				if(scheduleConfig.getPeriod() != null) {
					timer.schedule(timerTask, scheduleDate, scheduleConfig.getPeriod());
				}
				else{
					timer.schedule(timerTask, scheduleDate);
				}

			}
		}
		else  {
			if(delay == null){
				delay = 1000L;
			}
			if (scheduleConfig.getFixedRate() != null && scheduleConfig.getFixedRate()) {

				timer.scheduleAtFixedRate(timerTask, delay, scheduleConfig.getPeriod());
			} else {
				if(scheduleConfig.getPeriod() != null) {
					timer.schedule(timerTask, delay, scheduleConfig.getPeriod());
				}
				else{
					timer.schedule(timerTask, delay);
				}

			}
		}
	}
	private void innerimeSchedule(TimerScheduleConfig scheduleConfig ) throws Exception{
		//		scheduleImportData(dataTranPlugin.getBatchSize());

		scheduleTimer = new ScheduleTimer(scheduleConfig ,this);
		scheduleTimer.start();

	}
	public void timeSchedule() throws Exception {
		ScheduleConfig scheduleConfig = importContext.getScheduleConfig();
		if(scheduleConfig instanceof TimerScheduleConfig){
			innerimeSchedule((TimerScheduleConfig)scheduleConfig);
		}
		else{
			this.jdkTimeSchedule(scheduleConfig);
		}


	}

	/**
	 * 通过synchronized关键字，控制任务按顺序执行
	 */
	public synchronized void externalTimeSchedule()  {

		TaskContext taskContext = isEnablePluginTaskIntercept()?new TaskContext(importContext,targetImportContext):null;
		long importStartTime = System.currentTimeMillis();
		try {
			if(isEnablePluginTaskIntercept())
				preCall(taskContext);
			scheduleImportData( taskContext );
			if(isEnablePluginTaskIntercept())
				afterCall(taskContext);
		}
		catch (Exception e){
			if(isEnablePluginTaskIntercept())
				throwException(taskContext,e);
			logger.error("scheduleImportData failed:",e);
		}
		finally {
			long importEndTime = System.currentTimeMillis();
			if(importContext != null && this.importContext.isPrintTaskLog() && logger.isInfoEnabled())
				logger.info(new StringBuilder().append("Execute schedule job Take ").append((importEndTime - importStartTime)).append(" ms").toString());
		}


	}

//	private void startStoreStatusTask(){
//		storeStatusTask = new StoreStatusTask(this);
//		storeStatusTask.start();
//	}
	public void init(ImportContext importContext,ImportContext targetImportContext){
		this.importContext = importContext;
		this.targetImportContext = targetImportContext;
	}

	public void stop(){
		try {
			if (timer != null) {
				timer.cancel();
			}
			if(scheduleTimer != null)
			{
				scheduleTimer.stop();
			}
		}
		catch (Exception e){
			logger.error("",e);
		}
//		try {
//			this.storeStatusTask.interrupt();
//		}catch (Exception e){
//			logger.error("",e);
//		}

//		this.dataTranPlugin.destroy();

	}




}
