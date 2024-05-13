package org.frameworkset.tran.plugin.mongodb.output;
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

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.frameworkset.nosql.mongodb.MongoDB;
import org.frameworkset.nosql.mongodb.MongoDBHelper;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.cdc.TableMapping;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <p>Description: import datas to database task command</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class MongoDBMultiTargetTaskCommandImpl extends MongoDBTaskCommandImpl {

	private static final Logger logger = LoggerFactory.getLogger(MongoDBMultiTargetTaskCommandImpl.class);
	public MongoDBMultiTargetTaskCommandImpl(ImportCount importCount, ImportContext importContext,
	                                         List<CommonRecord> datas, int taskNo, String jobNo, String taskInfo,
	                                         LastValueWrapper lastValue, Status currentStatus,  TaskContext taskContext) {
		super(  importCount,   importContext,
				 datas,   taskNo,   jobNo,   taskInfo,
				  lastValue,   currentStatus,    taskContext);

	}




	protected Map<String,Map<String,DataMap>> splitRecords(){
		String objectIdField = dbOutputConfig.getObjectIdField();
		if(objectIdField == null){
			objectIdField = "_id";
		}
		String defualtTargetDatasource = dbOutputConfig.getName();
		Map<String,Map<String,DataMap>> datasourceDataMapMap = new LinkedHashMap<>();
		String defaultTable = dbOutputConfig.getDBCollection();
		String defaultDb = dbOutputConfig.getDB();
		StringBuilder defaultKey = new StringBuilder();
		defaultKey.append(defaultDb).append(":").append(defaultTable);
		String dk = defaultKey.toString();
		String mapKey = null;
		for(CommonRecord dbRecord:records){
			TableMapping tableMapping = dbRecord.getTableMapping();
			String tdb = tableMapping != null?tableMapping.getTargetDatabase():null;
			String tds = tableMapping != null?tableMapping.getTargetDatasource():null;
			String ttb = tableMapping != null?tableMapping.getTargetCollection():null;
			String table = null;
			if(ttb != null) {
				table = ttb ;
			}
			else{
				if(defaultTable != null)
				{
					table = defaultTable;
				}
				else{
					table = (String) dbRecord.getMetaValue("table");
				}

			}
			String database = null;
			if(tdb != null) {
				database = tdb ;
			}
			else{
				if(defaultDb != null)
				{
					database = defaultDb;
				}
				else{
					database = (String) dbRecord.getMetaValue("database");
				}

			}
			String targetDatasource = tds != null? tds:defualtTargetDatasource;
			if(targetDatasource == null || database == null || table == null){
				throw ImportExceptionUtil.buildDataImportException(importContext,new StringBuilder().append("Setting check failed:targetDatasource == ").append(targetDatasource)
						.append(" || database == ").append(database)
						.append(" || table == ").append(table)
						.append("").toString()
						);
			}
			Map<String,DataMap> dataMapMap = datasourceDataMapMap.get(targetDatasource);
			if(dataMapMap == null){
				dataMapMap = new LinkedHashMap<>();
				datasourceDataMapMap.put(targetDatasource,dataMapMap);
			}
			if(table != null && database != null){
				defaultKey.setLength(0);
				defaultKey.append(database).append(":").append(table);
				mapKey = defaultKey.toString();
			}
			else{
				table = defaultTable;
				database = defaultDb;
				mapKey = dk;
			}
			DataMap dataMap = dataMapMap.get(mapKey);
			if(dataMap == null){
				dataMap = new DataMap();
				dataMap.setDatabase(database);
				dataMap.setCollection(table);
				dataMapMap.put(mapKey,dataMap);
			}
			dataMap.addRecord(dbRecord,objectIdField);

		}
		return datasourceDataMapMap;
	}
	@Override
	public Object _execute( ){

		List<BulkWriteResult> results = null;
		try {
			Map<String,Map<String,DataMap>> dsDataMapMap = splitRecords();
			if(dsDataMapMap != null && dsDataMapMap.size() > 0){
				Iterator<Map.Entry<String, Map<String, DataMap>>> dsiterator = dsDataMapMap.entrySet().iterator();
				while (dsiterator.hasNext()){
					Map.Entry<String, Map<String, DataMap>> entry = dsiterator.next();

					Map<String,DataMap> dataMapMap = entry.getValue();
					if(dataMapMap != null && dataMapMap.size() > 0){
						String ds = entry.getKey();
						MongoDB mogodb = MongoDBHelper.getMongoDB(ds);
						Iterator<Map.Entry<String, DataMap>> iterator = dataMapMap.entrySet().iterator();
						while (iterator.hasNext()){
							DataMap dataMap = iterator.next().getValue();
							MongoDatabase db = mogodb.getDB(dataMap.getDatabase());
							MongoCollection dbCollection = db.getCollection(dataMap.getCollection());
							BulkWriteResult bulkWriteResult = dbCollection.bulkWrite(dataMap.getBulkOperations());
							if(results == null){
								results = new ArrayList<>();
							}
							results.add(bulkWriteResult);
						}
					}
				}
			}


		}
		catch (Exception e) {
			throw ImportExceptionUtil.buildDataImportException(importContext,taskInfo,e);
		}

		catch (Throwable e) {
			throw ImportExceptionUtil.buildDataImportException(importContext,taskInfo,e);
		} 
		return results;
	}




}
