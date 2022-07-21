package org.frameworkset.tran;/*
 *  Copyright 2008 biaoping.yin
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.ScheduleAssert;
import org.frameworkset.tran.schedule.ScheduleEndCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataStream {
	protected Logger logger = LoggerFactory.getLogger(this.getClass());

	protected ScheduleAssert scheduleAssert;

	public ImportContext getImportContext() {
		return importContext;
	}

	protected ImportContext importContext;
//	protected ImportContext targetImportContext;
//	protected BaseImportConfig importConfig ;
	public void startAction(){
		if(importContext.getImportStartAction() != null){
			try {
				importContext.getImportStartAction().startAction(importContext);
			}
			catch (Exception e){
				logger.warn("",e);
			}
		}
	}

	public void afterStartAction(){
		if(importContext.getImportStartAction() != null){
			try {
				importContext.getImportStartAction().afterStartAction(importContext);
			}
			catch (Exception e){
				logger.warn("",e);
			}
		}
	}
	public void setDataTranPlugin(DataTranPlugin dataTranPlugin) {
		this.dataTranPlugin = dataTranPlugin;


	}
	public void initDatastream(){
		startAction();
		this.dataTranPlugin.init(importContext);
		afterStartAction();
	}

	private DataTranPlugin dataTranPlugin;
	private boolean inited;
	//	public void setExternalTimer(boolean externalTimer) {
//		this.esjdbc.setExternalTimer(externalTimer);
//	}
	private Lock lock = new ReentrantLock();
//	public void setImportConfig(BaseImportConfig importConfig){
//		this.importConfig = importConfig;
//	}

//	public void setTargetImportContext(ImportContext targetImportContext) {
//		this.targetImportContext = targetImportContext;
//	}
	public void setImportContext(ImportContext importContext) {
		this.importContext = importContext;
	}

//	public ImportContext getTargetImportContext() {
//		return targetImportContext;
//	}

	/**
	 *
	 * @throws DataImportException
	 */
	public void execute() throws DataImportException {

		try {
			this.init();
//			importContext.importData();
//			DataTranPlugin dataTranPlugin = importContext.getDataTranPlugin();
			if(dataTranPlugin != null){

				dataTranPlugin.importData(new ScheduleEndCall() {
					@Override
					public void call() {
						//任务到期自动结束
						SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						String date = dateFormat.format(importContext.getScheduleEndDate());
						logger.info("Schedule job end date[{}] reached,schedule job stop begin....",date);
						DataStream.this.destroy(true);
						logger.info("Schedule job end date[{}] reached,schedule job stop complete.",date);
					}
				});

			}
//			if(this.scheduleService == null) {//一次性执行数据导入操作
//
//				long importStartTime = System.currentTimeMillis();
////				firstImportData();
//				this.dataTranPlugin.importData(new ImportContext() {
//
//				});
//				long importEndTime = System.currentTimeMillis();
//				if( this.dataTranPlugin.isPrintTaskLog() && logger.isInfoEnabled())
//					logger.info(new StringBuilder().append("Execute job Take ").append((importEndTime - importStartTime)).append(" ms").toString());
//			}
//			else{//定时增量导入数据操作
//				if(!this.dataTranPlugin.isExternalTimer()) {//内部定时任务引擎
//					scheduleService.timeSchedule();
//				}
//				else{ //外部定时任务引擎执行的方法，比如quartz之类的
//					scheduleService.externalTimeSchedule();
//				}
//			}
		}
		catch (Exception e) {

			endAction(e);
			throw new DataImportException(e);
		}
		finally{

		}
	}
	private boolean endActioned = false;
	public void endAction(Exception e){
		synchronized (this) {
			if (endActioned) {
				return;
			}
			endActioned = true;
		}
		if(this.importContext.getImportEndAction() != null){
			try {
				this.importContext.getImportEndAction().endAction(importContext,e);
			}
			catch (Exception ee){
				logger.warn("",ee);
			}
		}
	}
	public void destroy() {
		destroy(false);



//		this.esjdbc.stop();
	}

	/**
	 *
	 * @param waitTranStopped true 等待同步作业处理完成后停止作业 false 不等待
	 */
	public synchronized void destroy(boolean waitTranStopped) {

		destroy(waitTranStopped, false);

		endAction(null);

//		this.esjdbc.stop();
	}

	/**
	 *
	 * @param waitTranStopped true 等待同步作业处理完成后停止作业 false 不等待
	 * @param fromScheduleEnd 销毁操作是否来自于自动停止作业操作
	 */
	public synchronized void destroy(boolean waitTranStopped,boolean fromScheduleEnd) {

		if(importContext != null) {
			logger.info("Destroy DataStream begin,waitTranStopped[{}].",waitTranStopped);
			this.importContext.destroy(waitTranStopped, fromScheduleEnd);
			importContext = null;
			logger.info("DataStream stopped.");
		}
		else{
			logger.info("DataStream has stopped.");
		}



//		this.esjdbc.stop();
	}

	public String getConfigString() {
		return configString;
	}

	public void setConfigString(String configString) {
		this.configString = configString;
	}

	private String configString;


	public void init(){
		if(inited ) {
			importContext.resume();
			return;
		}
		if(importContext == null || importContext.getImportConfig() == null
				|| importContext.getInputConfig() == null || importContext.getOutputConfig() == null){
			throw new DataImportException("import Config is null.");
		}

		try {
			lock.lock();
			/** 1122
			this.importContext = this.buildImportContext(importConfig);
			 */
			/**
			dataTranPlugin = importContext.buildDataTranPlugin();

			dataTranPlugin.init();*/

//			this.initES(esjdbc.getApplicationPropertiesFile());
//			this.initDS(esjdbc.getDbConfig());
//			initOtherDSes(esjdbc.getConfigs());
//			this.initSQLInfo();
//			this.initSchedule();
			inited = true;
		}
		catch (Exception e) {
			inited = true;
			throw new DataImportException(e);
		}
		finally{


			lock.unlock();
		}
	}
	/**
	 * 暂停调度
	 */
	public boolean pauseSchedule(){
		if(scheduleAssert != null){
			return this.scheduleAssert.pauseSchedule();
		}
		return false;

	}
	/**
	 * 继续调度
	 */
	public boolean resumeSchedule(){
		if(scheduleAssert != null){
			return this.scheduleAssert.resumeSchedule();
		}
		return false;
	}

	public void setScheduleAssert(ScheduleAssert scheduleAssert) {
		this.scheduleAssert = scheduleAssert;
		if(dataTranPlugin != null)
			this.dataTranPlugin.setScheduleAssert(scheduleAssert);
	}
}
