package org.frameworkset.tran.plugin.mysqlbinlog.input;
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
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPluginImpl;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ContextImpl;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.ScheduleEndCall;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.InitLastValueClumnName;
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
public class MysqlBinlogDataTranPluginImpl extends DataTranPluginImpl {

	protected static Logger logger = LoggerFactory.getLogger(MysqlBinlogDataTranPluginImpl.class);

	private StoppedThread metricsThread;
    private MySQLBinlogConfig mySQLBinlogConfig;

	/**
	 * 默认十分钟执行一次拦截器监控数据afterCall方法
	 */
	private long metricsInterval ;
	public MysqlBinlogDataTranPluginImpl(ImportContext importContext){
		super(importContext);
        mySQLBinlogConfig = (MySQLBinlogConfig)importContext.getInputConfig();
		metricsInterval = mySQLBinlogConfig.getMetricsInterval();
		if(metricsInterval <= 0L){
			metricsInterval = DEFAULT_metricsInterval;
		}
	}

    @Override
    public SetLastValueType getSetLastValueType(){
        return new SetLastValueType (){

            public void set(){
                if( SimpleStringUtil.isEmpty(mySQLBinlogConfig.getFileNames())) {
                    importContext.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);
                    statusManager.initLastValueType();
                }
            }
        };
    }
    @Override
    public boolean useFilePointer(){
        return true;
    }
    @Override
    protected InitLastValueClumnName getInitLastValueClumnName(){
        return new InitLastValueClumnName (){

            public void initLastValueClumnName(){
                if(!SimpleStringUtil.isEmpty(mySQLBinlogConfig.getFileNames())){
                    statusManager.setIncreamentImport(false);
                }
                if(!mySQLBinlogConfig.isEnableIncrement()){
                    statusManager.setIncreamentImport(false);
                }
                else {
                    if (mySQLBinlogConfig.getPosition() == null) {
                        mySQLBinlogConfig.setPosition(0L);
                    }
                }
            }
        };
    }

    @Override
    public Context buildContext(TaskContext taskContext, TranResultSet tranResultSet, BatchContext batchContext){
        ContextImpl context = new ContextImpl(  taskContext,importContext, tranResultSet, batchContext);
        context.setAction(tranResultSet.getAction());
        return context;
    }

    protected boolean neadFinishJob(){
        return SimpleStringUtil.isNotEmpty(this.mySQLBinlogConfig.getFileNames());
    }
	@Override
	public void importData(ScheduleEndCall scheduleEndCall) throws DataImportException {


		long importStartTime = System.currentTimeMillis();
        MysqlBinlogTaskContext taskContext = inputPlugin.isEnablePluginTaskIntercept()?new MysqlBinlogTaskContext(importContext):null;
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
								taskContext.reInitContext(new MysqlBinlogTaskContext.ReInitAction(){

									@Override
									public void afterCall(TaskContext taskContext) {
                                        MysqlBinlogDataTranPluginImpl.this.afterCall(taskContext);
									}

									@Override
									public void preCall(TaskContext taskContext) {
                                        MysqlBinlogDataTranPluginImpl.this.preCall(taskContext);
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
            if(neadFinishJob()){//不扫码新文件
                importContext.finishAndWaitTran();
            }
		}
		catch (DataImportException dataImportException){
			throwException(taskContext,dataImportException);
            importContext.finishAndWaitTran();
        }
		catch (Exception dataImportException){
			throwException(taskContext,dataImportException);
            importContext.finishAndWaitTran();
            throw dataImportException;
		}
		catch (Throwable dataImportException){
			DataImportException e = new DataImportException(dataImportException);
			throwException(taskContext,new DataImportException(dataImportException));
            importContext.finishAndWaitTran();
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
	public void destroy(boolean waitTranStop, boolean fromScheduleEnd) {
        if(checkTranToStop()){
            return;
        }
		if(metricsThread != null)
			metricsThread.stopThread();
		super.destroy(waitTranStop, fromScheduleEnd);
	}
	//	@Override
//	public void initLastValueClumnName(){
//		setIncreamentImport(false);
//	}
}
