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

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
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
public  class MysqlBinlogInputDatatranPlugin extends BaseInputPlugin  {
	private static final Logger logger = LoggerFactory.getLogger(MysqlBinlogInputDatatranPlugin.class);
    private MySQLBinlogConfig mySQLBinlogConfig;
    private MySQLBinlogListener mySQLBinlogListener;
    private MysqlBinlogDataTranPluginImpl mysqlBinlogDataTranPlugin;
    public MysqlBinlogInputDatatranPlugin(ImportContext importContext){
        super(  importContext);
        mySQLBinlogConfig = (MySQLBinlogConfig) importContext.getInputConfig();
        this.jobType = "MysqlBinlogInputDatatranPlugin";
    }
    public boolean isEventMsgTypePlugin(){
        return true;
    }
	@Override
	public void init(){


	}

	@Override
	public void initStatusTableId() {
        if(dataTranPlugin.isIncreamentImport()) {
            StringBuilder id = new StringBuilder();
            if(mySQLBinlogConfig.getServerId() != null) {
                id.append(mySQLBinlogConfig.getServerId()).append(":").append(mySQLBinlogConfig.getHost()).append(":").append(mySQLBinlogConfig.getPort()).append("/")
                        .append(mySQLBinlogConfig.getDatabase()).append(mySQLBinlogConfig.getTables());

            }
            else{
                id.append(mySQLBinlogConfig.getHost()).append(":").append(mySQLBinlogConfig.getPort()).append("/")
                        .append(mySQLBinlogConfig.getDatabase()).append(mySQLBinlogConfig.getTables());
            }
            importContext.setStatusTableId(id.toString().hashCode());
        }
	}



	@Override
	public void beforeInit() {


	}


	@Override
	public void afterInit(){
        DBMetaUtil.initTableColumns(this.mySQLBinlogConfig);
        mysqlBinlogDataTranPlugin = (MysqlBinlogDataTranPluginImpl) dataTranPlugin;
	}



    @Override
	public void doImportData(TaskContext taskContext)  throws DataImportException {
        MysqlBinlogResultSet mysqlBinlogResultSet = new MysqlBinlogResultSet(this.importContext,this.mySQLBinlogConfig);
		final BaseDataTran mysqlBinlogDataTran = dataTranPlugin.createBaseDataTran( taskContext,mysqlBinlogResultSet,null,dataTranPlugin.getCurrentStatus());
//		dataTranPlugin.setHasTran();
		Thread tranThread = null;
		try {
			tranThread = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						mysqlBinlogDataTran.tran();
						dataTranPlugin.afterCall(taskContext);
//                        if(!mysqlBinlogDataTranPlugin.neadFinishJob()){
//                            importContext.finishAndWaitTran(null);
//                        }
					}
					catch (DataImportException dataImportException){
						logger.error("",dataImportException);
						dataTranPlugin.throwException(  taskContext,  dataImportException);
                        mysqlBinlogDataTran.stop2ndClearResultsetQueue(true);
                        importContext.finishAndWaitTran(dataImportException);
					}
					catch (RuntimeException dataImportException){
						logger.error("",dataImportException);
						dataTranPlugin.throwException(  taskContext,  dataImportException);
                        mysqlBinlogDataTran.stop2ndClearResultsetQueue(true);
                        importContext.finishAndWaitTran(dataImportException);
					}
					catch (Throwable dataImportException){
						logger.error("",dataImportException);
						DataImportException dataImportException_ = ImportExceptionUtil.buildDataImportException(importContext,dataImportException);
						dataTranPlugin.throwException(  taskContext, dataImportException_);
                        mysqlBinlogDataTran.stop2ndClearResultsetQueue(true);
                        importContext.finishAndWaitTran(dataImportException);
					}
				}
			},"mysqlbinlog-input-dataTran");
			tranThread.start();

			this.initMySQLBinlogListener(mysqlBinlogDataTran);
		} catch (DataImportException e) {
            if(mysqlBinlogDataTran != null)
                mysqlBinlogDataTran.stop2ndClearResultsetQueue(true);
			throw e;
		} catch (Exception e) {
            if(mysqlBinlogDataTran != null)
                mysqlBinlogDataTran.stop2ndClearResultsetQueue(true);
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}
        catch (Throwable e) {
            if(mysqlBinlogDataTran != null)
                mysqlBinlogDataTran.stop2ndClearResultsetQueue(true);
            throw ImportExceptionUtil.buildDataImportException(importContext,e);
        }


	}



    protected void initMySQLBinlogListener(BaseDataTran mysqlBinlogDataTran) throws Exception {
        final MySQLBinlogListener mySQLBinlogListener = new MySQLBinlogListener(mysqlBinlogDataTran,mySQLBinlogConfig,this.importContext);
        this.mySQLBinlogListener = mySQLBinlogListener;
        mySQLBinlogListener.start();

    }
    @Override
    public void stopCollectData(){
        try {
            if (mySQLBinlogListener != null) {
                mySQLBinlogListener.shutdown();
            }
        }
        catch (Exception e){
            logger.warn("",e);
        }
        super.stopCollectData();
    }
    @Override
    public void destroy(boolean waitTranStop) {



    }

    public String[] getColumns(String database, String table) {

        return this.mySQLBinlogConfig.getColumns(database,table);
    }
}
