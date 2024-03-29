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

import com.mongodb.BasicDBObject;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.frameworkset.nosql.mongodb.MongoDB;
import org.frameworkset.nosql.mongodb.MongoDBHelper;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: import datas to database task command</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class MongoDBTaskCommandImpl extends BaseTaskCommand<List<CommonRecord>, Object> {
	protected MongoDBOutputConfig dbOutputConfig;
	protected String taskInfo;
	private static final Logger logger = LoggerFactory.getLogger(MongoDBTaskCommandImpl.class);
	public MongoDBTaskCommandImpl(ImportCount importCount, ImportContext importContext,
                                  List<CommonRecord> datas, int taskNo, String jobNo, String taskInfo,
                                  LastValueWrapper lastValue, Status currentStatus,  TaskContext taskContext) {
		super(importCount,importContext, datas.size(),  taskNo,  jobNo,lastValue,  currentStatus,   taskContext);
		this.importContext = importContext;
		this.datas = datas;
		dbOutputConfig = (MongoDBOutputConfig) importContext.getOutputConfig();
		this.taskInfo = taskInfo;

	}



	public List<CommonRecord> getDatas() {
		return datas;
	}


	protected List<CommonRecord> datas;
	private int tryCount;



	public void setDatas(List<CommonRecord> datas) {
		this.datas = datas;
	}

	protected Object _execute(){
		MongoDB mogodb = MongoDBHelper.getMongoDB(dbOutputConfig.getName());
		MongoDatabase db = mogodb.getDB(dbOutputConfig.getDB());
		MongoCollection dbCollection = db.getCollection(dbOutputConfig.getDBCollection());

		String objectIdField = dbOutputConfig.getObjectIdField();
		if(objectIdField == null){
			objectIdField = "_id";
		}
		List<WriteModel<Document>> bulkOperations = new ArrayList<>();
		for(CommonRecord dbRecord:datas){
			CommonRecord record = dbRecord;
			DataMap.addRecord(bulkOperations,record,objectIdField);


		}
		if(bulkOperations.size() > 0){
			BulkWriteResult bulkWriteResult = dbCollection.bulkWrite(bulkOperations);
			return bulkWriteResult;

		}
		return null;
	}

	@Override
	public Object execute(){
		Object data = null;
		if(this.importContext.getMaxRetry() > 0){
			if(this.tryCount >= this.importContext.getMaxRetry())
				throw new TaskFailedException("task execute failed:reached max retry times "+this.importContext.getMaxRetry());
		}
		this.tryCount ++;

		try {



			Object bulkWriteResult =  _execute(  );
			if(bulkWriteResult != null){
				data = bulkWriteResult;
				finishTask();

			}

		}

		catch (Exception e) {

			throw new DataImportException(taskInfo,e);

		}

		catch (Throwable e) {

			throw new DataImportException(taskInfo,e);

		} 
		return data;
	}

	public int getTryCount() {
		return tryCount;
	}


}
