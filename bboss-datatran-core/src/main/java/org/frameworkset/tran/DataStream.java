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
import org.frameworkset.tran.context.JobContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.schedule.ScheduleAssert;
import org.frameworkset.tran.schedule.ScheduleEndCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
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
			catch (DataImportException e){
//				logger.warn("",e);
                throw e;
			}
            catch (Exception e){
//                logger.warn("",e);
                throw new DataImportException("startAction failed:",e);
            }
		}
	}

	public void afterStartAction(){
		if(importContext.getImportStartAction() != null){
			try {
				importContext.getImportStartAction().afterStartAction(importContext);
			}
            catch (DataImportException e){
//				logger.warn("",e);
                throw e;
            }
            catch (Exception e){
//                logger.warn("",e);
                throw new DataImportException("afterStartAction failed:",e);
            }
		}
	}
	public void setDataTranPlugin(DataTranPlugin dataTranPlugin) {
		this.dataTranPlugin = dataTranPlugin;
        this.getJobContext().setDataTranPlugin(dataTranPlugin);

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
    
    public JobContext getJobContext(){
        return importContext.getJobContext();
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
					public void call(boolean scheduled) {
                        if(scheduled) {
                            //任务到期自动结束
                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            String date = dateFormat.format(importContext.getScheduleEndDate());
                            logger.info("Schedule job end date[{}] reached,schedule job stop begin....", date);
                            DataStream.this.destroy(true);
                            logger.info("Schedule job end date[{}] reached,schedule job stop complete.", date);
                        }
                        else{
                            DataStream.this.destroy(true);
                        }
					}
				});

			}

		}
		catch (DataImportException e) {

			endAction(e);
			throw e;
		}
		catch (Exception e) {

			endAction(e);
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}

	}
	private boolean endActioned = false;
    private Object endActionLock = new Object();
	public void endAction(Exception e){
		synchronized (endActionLock) {
			if (endActioned) {
				return;
			}
            
			endActioned = true;
		}
		if(importContext != null && this.importContext.getImportEndActions() != null){
            List<ImportEndAction> importEndActions = importContext.getImportEndActions();
            for (ImportEndAction importEndAction: importEndActions) {
                try {
                    importEndAction.endAction(importContext, e);
                } catch (Exception ee) {
                    logger.warn("", ee);
                }
            }
            importContext.getJobContext().setEndStartTime(new Date());
		}
        
	}
	public void destroy() {
		destroy(false);
	}

    private Object destroyLock = new Object();
	/**
	 *
	 * @param waitTranStopped true 等待同步作业处理完成后停止作业 false 不等待
	 */
	public void destroy(boolean waitTranStopped) {

		destroy(waitTranStopped, false);

//		endAction(null);
//		importContext = null;
//		this.esjdbc.stop();
	}
    private boolean destoried ;

    public void destroy(DestroyPolicy destroyPolicy) {

        if(destoried){
            return ;
        }
        synchronized (destroyLock){
            if(destoried){
                return ;
            }
            destoried = true;
        }
        if(importContext != null) {
            logger.info("Destroy DataStream begin,waitTranStopped[{}].",destroyPolicy.isWaitTranStopped());
            this.importContext.destroy(  destroyPolicy);

            logger.info("DataStream stopped.");
        }
        else{
            logger.info("DataStream stopped.");
        }



    }
	/**
	 *
	 * @param waitTranStopped true 等待同步作业处理完成后停止作业 false 不等待
	 * @param fromScheduleEnd 销毁操作是否来自于自动停止作业操作
	 */
	public void destroy(boolean waitTranStopped,boolean fromScheduleEnd) {

        DestroyPolicy destroyPolicy = new DestroyPolicy();
        destroyPolicy.setWaitTranStopped(waitTranStopped);
        destroyPolicy.setFromScheduleEnd(fromScheduleEnd);
        destroy(destroyPolicy);



	}

	/**
	 *
	 * @param waitTranStopped true 等待同步作业处理完成后停止作业 false 不等待
	 * @param fromScheduleEnd 销毁操作是否来自于自动停止作业操作
	 */
	public void destroy(Throwable throwable,boolean waitTranStopped,boolean fromScheduleEnd) {

		DestroyPolicy destroyPolicy = new DestroyPolicy();
		destroyPolicy.setWaitTranStopped(waitTranStopped);
		destroyPolicy.setFromScheduleEnd(fromScheduleEnd);
		destroyPolicy.setThrowable(throwable);
		destroy(destroyPolicy);



	}

    /**
     * 暂时和destory()方法行为一致，未对forceStrop做处理
     */
    public void destroyForce() {

        DestroyPolicy destroyPolicy = new DestroyPolicy();
        destroyPolicy.setWaitTranStopped(false);
        destroyPolicy.setFromScheduleEnd(false);
        destroyPolicy.setForceStop(true);
        destroy(destroyPolicy);



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
			throw ImportExceptionUtil.buildDataImportException(importContext,"import Config is null.");
		}
		lock.lock();
		try {

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
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
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

    /**
     * 重置日志级别,无需重启作业
     * @param metricsLogLevel
     */
    public void resetMetricsLogLevel(int metricsLogLevel){
        this.importContext.resetMetricsLogLevel(  metricsLogLevel);
    }
}
