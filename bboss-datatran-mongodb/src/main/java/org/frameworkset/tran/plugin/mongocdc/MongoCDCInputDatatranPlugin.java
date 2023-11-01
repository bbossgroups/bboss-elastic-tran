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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.nosql.mongodb.MongoDBConfig;
import org.frameworkset.nosql.mongodb.MongoDBHelper;
import org.frameworkset.nosql.mongodb.MongoDBStartResult;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.mongodb.cdc.ReplicaSet;
import org.frameworkset.tran.plugin.BaseInputPlugin;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public  class MongoCDCInputDatatranPlugin extends BaseInputPlugin  {
	private static final Logger logger = LoggerFactory.getLogger(MongoCDCInputDatatranPlugin.class);
    private MongoCDCInputConfig mongoCDCInputConfig;
    private MongoDBCDCChangeStreamListener mongoDBCDCChangeStreamListener;
    private MongoCDCDataTranPluginImpl mongoCDCDataTranPlugin;
    public MongoCDCInputDatatranPlugin(ImportContext importContext){
        super(  importContext);
        mongoCDCInputConfig = (MongoCDCInputConfig) importContext.getInputConfig();
        this.jobType = "MongoCDCInputDatatranPlugin";
    }
    public boolean isEventMsgTypePlugin(){
        return true;
    }
    private MongoDBStartResult mongoDBStartResult = new MongoDBStartResult();
    @Override
    public void init(){


    }

    @Override
    public void destroy(boolean waitTranStop) {
        Map<String,Object> dbs = mongoDBStartResult.getDbstartResult();
        if (dbs != null && dbs.size() > 0){
            Iterator<Map.Entry<String, Object>> iterator = dbs.entrySet().iterator();
            while(iterator.hasNext()){
                Map.Entry<String, Object> entry = iterator.next();
                MongoDBHelper.closeDB(entry.getKey());
            }
        }
    }



    @Override
    public void beforeInit() {
        initMongoDB();


    }

    protected void initMongoDB(){
        MongoDBConfig mongoDBConfig = new MongoDBConfig();
        mongoDBConfig.setName(mongoCDCInputConfig.getName());
        mongoDBConfig.setCredentials(mongoCDCInputConfig.getCredentials());
        mongoDBConfig.setServerAddresses(mongoCDCInputConfig.getServerAddresses());
        mongoDBConfig.setOption(mongoCDCInputConfig.getOption());//private String option;
        mongoDBConfig.setWriteConcern(mongoCDCInputConfig.getWriteConcern());//private String writeConcern;
        mongoDBConfig.setReadPreference(mongoCDCInputConfig.getReadPreference());//private String readPreference;
        mongoDBConfig.setAutoConnectRetry(mongoCDCInputConfig.getAutoConnectRetry());//private Boolean autoConnectRetry = true;

        mongoDBConfig.setConnectionsPerHost(mongoCDCInputConfig.getConnectionsPerHost());//private int connectionsPerHost = 50;

        mongoDBConfig.setMaxWaitTime(mongoCDCInputConfig.getMaxWaitTime());//private int maxWaitTime = 120000;
        mongoDBConfig.setSocketTimeout(mongoCDCInputConfig.getSocketTimeout());//private int socketTimeout = 0;
        mongoDBConfig.setConnectTimeout(mongoCDCInputConfig.getConnectTimeout());//private int connectTimeout = 15000;


        /**是否启用sql日志，true启用，false 不启用，*/
        mongoDBConfig.setThreadsAllowedToBlockForConnectionMultiplier(mongoCDCInputConfig.getThreadsAllowedToBlockForConnectionMultiplier());//private int threadsAllowedToBlockForConnectionMultiplier;
        mongoDBConfig.setSocketKeepAlive(mongoCDCInputConfig.getSocketKeepAlive());//private Boolean socketKeepAlive = false;

        mongoDBConfig.setMode( mongoCDCInputConfig.getMode());
        mongoDBConfig.setConnectString(mongoCDCInputConfig.getConnectString());
        if(MongoDBHelper.init(mongoDBConfig)){
            mongoDBStartResult.addDBStartResult(mongoDBConfig.getName());
        }
    }

	@Override
	public void initStatusTableId() {
        if(dataTranPlugin.isIncreamentImport()) {
            StringBuilder id = new StringBuilder();
            if(mongoCDCInputConfig.getConnectString() != null) {
                id.append(mongoCDCInputConfig.getConnectString());

            }
            else if(mongoCDCInputConfig.getServerAddresses() != null){
                id.append(mongoCDCInputConfig.getConnectString());
            }
            if(mongoCDCInputConfig.getCollectionIncludeList() != null){
                if(id.length() > 0 )
                    id.append(":");
                id.append(mongoCDCInputConfig.getCollectionIncludeList());

            }
            if(mongoCDCInputConfig.getCollectionExcludeList() != null){
                if(id.length() > 0 )
                    id.append(":");
                id.append(mongoCDCInputConfig.getCollectionExcludeList());
            }
            if(mongoCDCInputConfig.getDbExcludeList() != null){
                if(id.length() > 0 )
                    id.append(":");
                id.append(mongoCDCInputConfig.getDbExcludeList());
            }

            if(mongoCDCInputConfig.getDbIncludeList() != null){
                if(id.length() > 0 )
                    id.append(":");
                id.append(mongoCDCInputConfig.getDbIncludeList());
            }
            importContext.setStatusTableId(id.toString().hashCode());
        }
	}






	@Override
	public void afterInit(){
        mongoCDCDataTranPlugin = (MongoCDCDataTranPluginImpl) dataTranPlugin;
	}



    @Override
	public void doImportData(TaskContext taskContext)  throws DataImportException {
        MongoCDCResultSet mongoCDCResultSet = new MongoCDCResultSet(this.importContext,this.mongoCDCInputConfig);
		final BaseDataTran baseDataTran = dataTranPlugin.createBaseDataTran( taskContext,mongoCDCResultSet,null,dataTranPlugin.getCurrentStatus());
//		dataTranPlugin.setHasTran();
		Thread tranThread = null;
		try {
			tranThread = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						baseDataTran.tran();
						dataTranPlugin.afterCall(taskContext);
//                        if(!mysqlBinlogDataTranPlugin.neadFinishJob()){
//                            importContext.finishAndWaitTran(null);
//                        }
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
						DataImportException dataImportException_ = new DataImportException(dataImportException);
						dataTranPlugin.throwException(  taskContext, dataImportException_);
                        baseDataTran.stop2ndClearResultsetQueue(true);
                        importContext.finishAndWaitTran(dataImportException);
					}
				}
			},"mysqlbinlog-input-dataTran");
			tranThread.start();

			this.initMongoDBChangeStreamListener(baseDataTran);
		} catch (DataImportException e) {
            if(baseDataTran != null)
                baseDataTran.stop2ndClearResultsetQueue(true);
			throw e;
		} catch (Exception e) {
            if(baseDataTran != null)
                baseDataTran.stop2ndClearResultsetQueue(true);
			throw new DataImportException(e);
		}
        catch (Throwable e) {
            if(baseDataTran != null)
                baseDataTran.stop2ndClearResultsetQueue(true);
            throw new DataImportException(e);
        }


	}



    protected void initMongoDBChangeStreamListener(BaseDataTran mysqlBinlogDataTran) throws Exception {
        MongoDBCDCChangeStreamListener mongoDBCDCChangeStreamListener = null;
        if(SimpleStringUtil.isNotEmpty(mongoCDCInputConfig.getConnectString()))
            mongoDBCDCChangeStreamListener = new MongoDBCDCChangeStreamListener(new ReplicaSet(mongoCDCInputConfig.getConnectString()),mongoCDCInputConfig,mysqlBinlogDataTran, this.importContext);
        else
            mongoDBCDCChangeStreamListener = new MongoDBCDCChangeStreamListener(mongoCDCInputConfig,mysqlBinlogDataTran, this.importContext);
        this.mongoDBCDCChangeStreamListener = mongoDBCDCChangeStreamListener;
        mongoDBCDCChangeStreamListener.start();

    }
    @Override
    public void stopCollectData(){
        try {
            if (mongoDBCDCChangeStreamListener != null) {
                mongoDBCDCChangeStreamListener.shutdown();
            }
        }
        catch (Exception e){
            logger.warn("",e);
        }
        super.stopCollectData();
    }


}
