package org.frameworkset.tran.plugin.mongodb.input;
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
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.frameworkset.nosql.mongodb.MongoDB;
import org.frameworkset.nosql.mongodb.MongoDBConfig;
import org.frameworkset.nosql.mongodb.MongoDBHelper;
import org.frameworkset.nosql.mongodb.MongoDBStartResult;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.mongodb.MongoDBResultSet;
import org.frameworkset.tran.plugin.BaseInputPlugin;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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
public class MongoDBInputDatatranPlugin extends BaseInputPlugin {
	private static Logger logger = LoggerFactory.getLogger(MongoDBInputDatatranPlugin.class);
	private MongoDBInputConfig mongoDBInputConfig;
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


	public MongoDBInputDatatranPlugin(ImportContext importContext){
		super(  importContext);
		mongoDBInputConfig = (MongoDBInputConfig) importContext.getInputConfig();
		this.jobType = "MongoDBInputDatatranPlugin";

	}

	@Override
	public void beforeInit() {
		initMongoDB();


	}

	protected void initMongoDB(){
		MongoDBConfig mongoDBConfig = new MongoDBConfig();
		mongoDBConfig.setName(mongoDBInputConfig.getName());
		mongoDBConfig.setCredentials(mongoDBInputConfig.getCredentials());
		mongoDBConfig.setServerAddresses(mongoDBInputConfig.getServerAddresses());
		mongoDBConfig.setOption(mongoDBInputConfig.getOption());//private String option;
		mongoDBConfig.setWriteConcern(mongoDBInputConfig.getWriteConcern());//private String writeConcern;
		mongoDBConfig.setReadPreference(mongoDBInputConfig.getReadPreference());//private String readPreference;

		mongoDBConfig.setConnectionsPerHost(mongoDBInputConfig.getConnectionsPerHost());//private int connectionsPerHost = 50;

		mongoDBConfig.setMaxWaitTime(mongoDBInputConfig.getMaxWaitTime());//private int maxWaitTime = 120000;
		mongoDBConfig.setSocketTimeout(mongoDBInputConfig.getSocketTimeout());//private int socketTimeout = 0;
		mongoDBConfig.setConnectTimeout(mongoDBInputConfig.getConnectTimeout());//private int connectTimeout = 15000;


		mongoDBConfig.setSocketKeepAlive(mongoDBInputConfig.getSocketKeepAlive());//private Boolean socketKeepAlive = false;

		mongoDBConfig.setMode( mongoDBInputConfig.getMode());
		mongoDBConfig.setConnectString(mongoDBInputConfig.getConnectString());

        mongoDBConfig.setCustomSettingBuilder(mongoDBInputConfig.getCustomSettingBuilder());
		if(MongoDBHelper.init(mongoDBConfig)){
			mongoDBStartResult.addDBStartResult(mongoDBConfig.getName());
		}
	}
	@Override
	public void afterInit(){

	}

	@Override
	public void initStatusTableId() {
		if(importContext.isIncreamentImport()) {
			//计算增量记录id

			String statusTableId = mongoDBInputConfig.getDB()+"|"+mongoDBInputConfig.getDBCollection()+"|"+mongoDBInputConfig.getServerAddresses();
			if(mongoDBInputConfig.getQuery() != null){
				statusTableId = statusTableId +"|" + mongoDBInputConfig.getQuery().toString();
			}
			importContext.setStatusTableId(statusTableId.hashCode());
		}

	}

	private void commonImportData( TaskContext taskContext) throws Exception {

		BasicDBObject dbObject = mongoDBInputConfig.getQuery();
		if(dbObject == null)
			dbObject = new BasicDBObject();

		exportESData(  dbObject, taskContext);
		/**
		 * JDBCResultSet jdbcResultSet = new JDBCResultSet();
		 * 		jdbcResultSet.setResultSet(resultSet);
		 * 		jdbcResultSet.setMetaData(statementInfo.getMeta());
		 * 		jdbcResultSet.setDbadapter(statementInfo.getDbadapter());
		 * 		DB2ESDataTran db2ESDataTran = new DB2ESDataTran(jdbcResultSet,importContext);
		 *
		 * 		db2ESDataTran.tran(  );
		 */
	}

	private void exportESData(BasicDBObject dbObject, TaskContext taskContext){
		MongoDB mogodb = MongoDBHelper.getMongoDB(mongoDBInputConfig.getName());
		MongoDatabase db = mogodb.getDB(mongoDBInputConfig.getDB());
		MongoCollection<Document> dbCollection = db.getCollection(mongoDBInputConfig.getDBCollection());
		/**
		DBCollectionFindOptions dbCollectionFindOptions = null;
		if(mongoDBInputConfig.getDbCollectionFindOptions() != null){
			dbCollectionFindOptions = mongoDBInputConfig.getDbCollectionFindOptions();
			dbCollectionFindOptions.batchSize(importContext.getFetchSize());
		}
		else
		{
			dbCollectionFindOptions = new DBCollectionFindOptions();
			dbCollectionFindOptions.batchSize(importContext.getFetchSize());
		}
*/

//		dbCollectionFindOptions.

		FindIterable<Document> dbCursor = dbCollection.find(dbObject);
		if(SimpleStringUtil.isNotEmpty(mongoDBInputConfig.getFetchFields() )){
			Bson projectionFields = Projections.fields(
					Projections.include(mongoDBInputConfig.getFetchFields())
//					,Projections.excludeId()
			);
			dbCursor.projection(projectionFields);
//			dbCollectionFindOptions.projection(mongoDBInputConfig.getFetchFields());
		}



		if(importContext.getFetchSize()> 0)
			dbCursor.batchSize(importContext.getFetchSize());

//		MongoDBResultSet mongoDB2ESResultSet = new MongoDBResultSet(importContext,dbCursor);
//		MongoDB2ESDataTran mongoDB2ESDataTran = new MongoDB2ESDataTran(mongoDB2ESResultSet,importContext);
//		mongoDB2ESDataTran.tran();
		MongoCursor mongoCursor = null;
		try {
			mongoCursor = dbCursor.cursor();
			doTran(mongoCursor, taskContext);
		}
		finally {
			if(mongoCursor != null){
				mongoCursor.close();
			}
		}

	}
	protected  void doTran(MongoCursor dbCursor, TaskContext taskContext){
		MongoDBResultSet mongoDB2ESResultSet = new MongoDBResultSet(importContext,dbCursor);
		BaseDataTran mongoDB2ESDataTran = dataTranPlugin.createBaseDataTran(taskContext,mongoDB2ESResultSet,null,dataTranPlugin.getCurrentStatus());//new BaseElasticsearchDataTran( taskContext,mongoDB2ESResultSet,importContext,targetImportContext,this.currentStatus);
		mongoDB2ESDataTran.initTran();
        dataTranPlugin.callTran( mongoDB2ESDataTran);
	}
	private void increamentImportData( TaskContext taskContext) throws Exception {

		BasicDBObject dbObject = mongoDBInputConfig.getQuery();
		if(dbObject == null)
			dbObject = new BasicDBObject();
		putLastParamValue(dbObject);
		exportESData(  dbObject, taskContext);
	}
	public void putLastParamValue(BasicDBObject query){
		int lastValueType = dataTranPlugin.getLastValueType();
		Status currentStatus = dataTranPlugin.getCurrentStatus();
        LastValueWrapper currentLastValueWrapper = currentStatus.getCurrentLastValueWrapper();
        Object lastValue = currentLastValueWrapper.getLastValue();
		if(lastValueType == ImportIncreamentConfig.NUMBER_TYPE) {
			query.append(getLastValueVarName(),
					new BasicDBObject("$gt", lastValue));
		}
		else{
			Object lv = null;
			if(lastValue instanceof Date) {
				lv = lastValue;
			}
			else {
				if(lastValue instanceof Long) {
					lv =   new Date((Long)lastValue);
				}
				else if(lastValue instanceof Integer){
					lv =  new Date(((Integer) lastValue).longValue());
				}
				else if(lastValue instanceof Short){
					lv =  new Date(((Short) lastValue).longValue());
				}
				else{
					lv =  new Date(((Number) lastValue).longValue());
				}
			}

			if(importContext.increamentEndOffset() != null){
				Date lastOffsetValue = TimeUtil.addDateSeconds(new Date(),0-importContext.increamentEndOffset());
				BasicDBObject basicDBObject = new BasicDBObject();
				basicDBObject.put("$gt", lv);
				basicDBObject.put("$lte",lastOffsetValue);

				query.append(getLastValueVarName(), basicDBObject);
			}
			else{
				query.append(getLastValueVarName(),
						new BasicDBObject("$gt", lv));
			}
		}
		if(importContext.isPrintTaskLog()){
			logger.info(new StringBuilder().append("Current values: ").append(query).toString());
		}
	}

	public void doImportData( TaskContext taskContext)  throws DataImportException {


			try {
				if (!importContext.isIncreamentImport()) {

					commonImportData(   taskContext );

				} else {

					increamentImportData(   taskContext );

				}
			} catch (DataImportException e) {
				throw e;
			} catch (Exception e) {
				throw ImportExceptionUtil.buildDataImportException(importContext,e);
			}

	}


}
