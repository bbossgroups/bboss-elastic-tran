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

import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataStream {
	protected Logger logger = LoggerFactory.getLogger(this.getClass());


	public ImportContext getImportContext() {
		return importContext;
	}

	protected ImportContext importContext;
	protected ImportContext targetImportContext;
	protected BaseImportConfig importConfig ;

	public void setDataTranPlugin(DataTranPlugin dataTranPlugin) {
		this.dataTranPlugin = dataTranPlugin;
		try {
			this.dataTranPlugin.init(importContext,targetImportContext);
			this.dataTranPlugin.init();
		}
		catch (ESDataImportException e){
			logger.error("Set DataTranPlugin failed:",e);
			throw e;
		}
		catch (Exception e){
			logger.error("Set DataTranPlugin failed:",e);
			throw new ESDataImportException("Set DataTranPlugin failed:",e);
		}

	}

	private DataTranPlugin dataTranPlugin;
	private boolean inited;
	//	public void setExternalTimer(boolean externalTimer) {
//		this.esjdbc.setExternalTimer(externalTimer);
//	}
	private Lock lock = new ReentrantLock();
	public void setImportConfig(BaseImportConfig importConfig){
		this.importConfig = importConfig;
	}

	public void setTargetImportContext(ImportContext targetImportContext) {
		this.targetImportContext = targetImportContext;
	}
	public void setImportContext(ImportContext importContext) {
		this.importContext = importContext;
	}

	public ImportContext getTargetImportContext() {
		return targetImportContext;
	}

	/**
	 *
	 * @throws ESDataImportException
	 */
	public void execute() throws ESDataImportException {

		try {
			this.init();
//			importContext.importData();
			DataTranPlugin dataTranPlugin = importContext.getDataTranPlugin();
			if(dataTranPlugin != null){
				dataTranPlugin.importData();
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
			throw new ESDataImportException(e);
		}
		finally{

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
	public void destroy(boolean waitTranStopped) {

		if(importContext != null) {
			logger.info("Destroy DataStream begin,waitTranStopped[{}].",waitTranStopped);
			this.importContext.destroy(waitTranStopped);
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
		if(importConfig == null){
			throw new ESDataImportException("import Config is null.");
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
			throw new ESDataImportException(e);
		}
		finally{


			lock.unlock();
		}
	}
}
