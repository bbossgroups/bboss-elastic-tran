package org.frameworkset.tran.plugin.mongodb.output;
/**
 * Copyright 2023 bboss
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
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.frameworkset.tran.CommonRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2023/11/3
 */
public class DataMap {
	private static Logger logger = LoggerFactory.getLogger(DataMap.class);
	private String database;
	private String collection;
	private List<WriteModel<Document>> bulkOperations;


	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public static Document convert(CommonRecord dbRecord,boolean includeObjectId){
		Document basicDBObject = new Document();
		Map<String, Object> datas = dbRecord.getDatas();
		Iterator<Map.Entry<String,Object>> iterator = datas.entrySet().iterator();
		while (iterator.hasNext()){
			Map.Entry<String,Object> entry = iterator.next();
			String key = entry.getKey();
			if(!includeObjectId && key.equals("_id"))
				continue;
			basicDBObject.append(entry.getKey(),entry.getValue());
		}
		return basicDBObject;
	}

	public static Bson convertUpdate(CommonRecord dbRecord,boolean includeObjectId){
		Document basicDBObject = new Document();
		Map<String, Object> datas = dbRecord.getDatas();
		Iterator<Map.Entry<String,Object>> iterator = datas.entrySet().iterator();
		while (iterator.hasNext()){
			Map.Entry<String,Object> entry = iterator.next();
			String key = entry.getKey();
			if(!includeObjectId && key.equals("_id"))
				continue;
			basicDBObject.append(entry.getKey(),entry.getValue());
		}
		BasicDBObject obj = new BasicDBObject();
		if(basicDBObject.size() > 0)
			obj.append("$set",basicDBObject);
		return obj;
	}
	public DataMap addRecord(CommonRecord dbRecord,String defaultObjectIdField){
		if(bulkOperations == null)
			bulkOperations = new ArrayList<>();
		addRecord( bulkOperations, dbRecord,  defaultObjectIdField);
		return this;
	}

	public static void addRecord(List<WriteModel<Document>> bulkOperations,CommonRecord dbRecord,String defaultObjectIdField){
		String objectIdField = null;
		if(dbRecord.getRecordKeyField() != null){
			objectIdField = dbRecord.getRecordKeyField();
		}
		else{
			objectIdField = defaultObjectIdField;
		}

		if(dbRecord.isInsert()) {
			Document basicDBObject = convert(dbRecord,true);

			bulkOperations.add(new InsertOneModel<>(basicDBObject));
		}
		else if(dbRecord.isUpdate()){
			Bson basicDBObject = convertUpdate( dbRecord,false);
			bulkOperations.add(new UpdateOneModel<Document>(Filters.eq("_id", dbRecord.getData(objectIdField)), basicDBObject));
		}
		else if(dbRecord.isDelete()){
			bulkOperations.add(new DeleteOneModel<>(Filters.eq("_id", dbRecord.getData(objectIdField))));
		}
		else{
			logger.info("Record action:{} is not supported,data:{}",dbRecord.getAction(), SimpleStringUtil.object2json(dbRecord.getDatas()));
		}
	}

	public List<WriteModel<Document>> getBulkOperations() {
		return bulkOperations;
	}


}
