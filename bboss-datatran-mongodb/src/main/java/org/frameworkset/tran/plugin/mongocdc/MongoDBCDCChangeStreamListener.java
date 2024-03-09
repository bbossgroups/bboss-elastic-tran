package org.frameworkset.tran.plugin.mongocdc;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.*;
import org.bson.*;
import org.bson.types.ObjectId;
import org.frameworkset.nosql.mongodb.MongoDB;
import org.frameworkset.nosql.mongodb.MongoDBHelper;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.mongodb.cdc.ChangeStreamPipeline;
import org.frameworkset.tran.mongodb.cdc.ChangeStreamPipelineFactory;
import org.frameworkset.tran.mongodb.cdc.ReplicaSet;
import org.frameworkset.tran.record.CommonData;
import org.frameworkset.tran.schedule.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;


public class MongoDBCDCChangeStreamListener {
    private static final Logger logger = LoggerFactory.getLogger(MongoDBCDCChangeStreamListener.class);
    private BaseDataTran dataTran;
    private boolean isIncreament;
    private ImportContext importContext;
    private MongoCDCInputConfig mongoCDCInputConfig;
    private boolean statusRunning;
    private ReplicaSet replicaSet;
    private String lastDroppedPostion;


    private Object lock = new Object();
    private ChangeStreamIterable<Document> changeStreamIterable;
    public MongoDBCDCChangeStreamListener( MongoCDCInputConfig mongoCDCInputConfig, BaseDataTran dataTran, ImportContext importContext){
        this.mongoCDCInputConfig = mongoCDCInputConfig;
        this.dataTran = dataTran;
        this.importContext = importContext;
        this.isIncreament = mongoCDCInputConfig.isEnableIncrement();
    }

    public MongoDBCDCChangeStreamListener(ReplicaSet replicaSet, MongoCDCInputConfig mongoCDCInputConfig, BaseDataTran dataTran, ImportContext importContext){
        this.mongoCDCInputConfig = mongoCDCInputConfig;
        this.dataTran = dataTran;
        this.importContext = importContext;
        this.replicaSet =replicaSet;
        this.isIncreament = mongoCDCInputConfig.isEnableIncrement();
    }
    
    private Map<String,Object> convertData(MongoDBCDCData mongoDBCDCData,UpdateDescription bsonDocument){
        if(bsonDocument == null)
            return null;
        Map<String,Object> data = new LinkedHashMap<>();
        BsonDocument updateFields = bsonDocument.getUpdatedFields();
        if(updateFields != null) {
            Set<Map.Entry<String, BsonValue>> entries = updateFields.entrySet();

            Iterator<Map.Entry<String, BsonValue>> iterator = entries.iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, BsonValue> entry = iterator.next();
                BsonValue bsonValue = entry.getValue();
                if(bsonValue.isString()){
                    data.put(entry.getKey(),  bsonValue.asString().getValue());

                }
                else if(bsonValue.isInt32()){
                    data.put(entry.getKey(),  bsonValue.asInt32().getValue());
                }
                else if(bsonValue.isInt64()){
                    data.put(entry.getKey(),  bsonValue.asInt64().getValue());
                }
                else if(bsonValue.isDateTime()){
                    data.put(entry.getKey(),  new Date(bsonValue.asDateTime().getValue()));
                }
                else if(bsonValue.isDouble()){
                    data.put(entry.getKey(),  bsonValue.asDouble().getValue());
                }
                else if(bsonValue.isBoolean()){
                    data.put(entry.getKey(),  bsonValue.asBoolean().getValue());
                }
                else if(bsonValue.isNull()){
//                    data.put(entry.getKey(),  bsonValue.asNumber().);
                }
                else if(bsonValue.isArray()){
                    data.put(entry.getKey(),  bsonValue.asArray().toArray());
                }
                
                else {
                    data.put(entry.getKey(), bsonValue);

                }
               
            }
        }

        List<String> removedFields = bsonDocument.getRemovedFields();
        if(removedFields != null && removedFields.size() > 0) {
//            data.put("mongodb.update.removedFields",removedFields);
            mongoDBCDCData.setRemovedFields(removedFields);
            
             
        }
        mongoDBCDCData.setUpdateDescription(bsonDocument);
//        data.put("mongodb.updateDescription",bsonDocument);
        data.put("_id",mongoDBCDCData.getId());
        return data;
    }

    private Map<String,Object> convertData(Document bsonDocument){
        if(bsonDocument == null)
            return null;
        Map<String,Object> data = new LinkedHashMap<>();
        Set<Map.Entry<String, Object>> entries = bsonDocument.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Object> entry = iterator.next();
            Object bsonValue = entry.getValue();
            if(bsonValue instanceof ObjectId) {
                data.put(entry.getKey(), ((ObjectId)bsonValue).toString());
            }
            else{
                data.put(entry.getKey(), bsonValue);
            }

        }
        return data;
    }
    private MongoDBCDCData buildInsertDocument(ChangeStreamDocument<Document> event){
        MongoDBCDCData mongoDBCDCData = new MongoDBCDCData();
        Document bsonDocument = event.getFullDocument();
	    evalId(  event.getDocumentKey(),  mongoDBCDCData );
        mongoDBCDCData.setData(convertData(  bsonDocument));
        mongoDBCDCData.setAction(Record.RECORD_INSERT);
        return mongoDBCDCData;

    }

    private MongoDBCDCData buildUpdateDocument(ChangeStreamDocument<Document> event){
        MongoDBCDCData mongoDBCDCData = new MongoDBCDCData();
        

        mongoDBCDCData.setAction(Record.RECORD_UPDATE);
	    evalId( event.getDocumentKey(), mongoDBCDCData );
        Document bsonDocument = event.getFullDocument();
        if(bsonDocument != null) {
            mongoDBCDCData.setData(convertData(bsonDocument));
        }
        else{
            mongoDBCDCData.setData(convertData(mongoDBCDCData,event.getUpdateDescription()));
        }
        
        Document updateDocument = event.getFullDocumentBeforeChange();
        if(updateDocument != null)
            mongoDBCDCData.setOldValues(convertData(updateDocument));
        return mongoDBCDCData;

    }

    private MongoDBCDCData buildReplaceDocument(ChangeStreamDocument<Document> event){
        MongoDBCDCData mongoDBCDCData = new MongoDBCDCData();
        

        mongoDBCDCData.setAction(Record.RECORD_REPLACE);
        evalId( event.getDocumentKey(), mongoDBCDCData );
        Document bsonDocument = event.getFullDocument();
        if(bsonDocument != null) {
            mongoDBCDCData.setData(convertData(bsonDocument));
        }
        else{
            mongoDBCDCData.setData(convertData(mongoDBCDCData,event.getUpdateDescription()));
        }
        Document updateDocument = event.getFullDocumentBeforeChange();
        if(updateDocument != null)
            mongoDBCDCData.setOldValues(convertData(updateDocument));
        return mongoDBCDCData;

    }


    private void evalId(BsonDocument bsonDocument,MongoDBCDCData mongoDBCDCData ){
        BsonValue id = bsonDocument.get("_id");
		String _id = null;
      
        if(id.isObjectId()){
            _id = id.asObjectId().getValue().toString();
        }
        else if(id.isString()){
            _id = id.asString().getValue();
        }
        else{
            _id = id.toString();
        }
		mongoDBCDCData.setId(_id);
	}

    private MongoDBCDCData buildDeleteDocument(ChangeStreamDocument<Document> event){
        MongoDBCDCData mongoDBCDCData = new MongoDBCDCData();
        Document bsonDocument = event.getFullDocument();
        if(bsonDocument != null) {
	        evalId(  event.getDocumentKey(),  mongoDBCDCData );
            mongoDBCDCData.setData(convertData(bsonDocument));
        }
        else{
	        BsonValue id = event.getDocumentKey().get("_id");
			String _id = null;
			if(id.isObjectId()){
				_id = id.asObjectId().getValue().toString();
			}
			else if(id.isString()){
				_id = id.asString().getValue();
			}
			else{
				_id = id.toString();
			}

            mongoDBCDCData.setId(_id);
            Map data = new LinkedHashMap();
            data.put("_id",mongoDBCDCData.getId());
            mongoDBCDCData.setData(data);
        }
        Document updateDocument = event.getFullDocumentBeforeChange();
        if(updateDocument != null)
            mongoDBCDCData.setOldValues(convertData(updateDocument));
        mongoDBCDCData.setAction(Record.RECORD_DELETE);
        return mongoDBCDCData;
    }
    private String getPosition(BsonDocument resumeToken){
        String position =resumeToken.get("_data").asString().getValue();
        return position;
    }
    private void dipatcheData(ChangeStreamDocument<Document> event) throws InterruptedException {
        BsonDocument resumeToken = event.getResumeToken();
        String position = getPosition(resumeToken);
//        BsonDocument documentKey = event.getDocumentKey();
        String db = event.getNamespace().getDatabaseName();
        String collection = event.getNamespace().getCollectionName();
        OperationType operationType = event.getOperationType();
        MongoDBCDCData mongoDBCDCData = null;
        if(operationType == OperationType.INSERT){
            mongoDBCDCData = this.buildInsertDocument(event);
        }
        else  if(operationType == OperationType.UPDATE ){
            mongoDBCDCData = this.buildUpdateDocument(event);
        }
        else  if( operationType == OperationType.REPLACE){
            mongoDBCDCData = this.buildReplaceDocument(event);
        }
        else  if(operationType == OperationType.DELETE){
            mongoDBCDCData = this.buildDeleteDocument(event);
        }
        else{
            mongoDBCDCData = buildDropedEvent( event);
        }

        if(mongoDBCDCData != null) {
            mongoDBCDCData.setCollection(collection);
            mongoDBCDCData.setDatabase(db);
            mongoDBCDCData.setPosition(position);
            mongoDBCDCData.setClusterTime(event.getClusterTime().asTimestamp().getValue());
            mongoDBCDCData.setWallTime(event.getWallTime().getValue());

            List<MongoCDCRecord> mongoCDCRecords = new ArrayList<>(1);
            MongoCDCRecord mongoCDCRecord = new MongoCDCRecord(dataTran.getTaskContext(),importContext,
                    mongoDBCDCData, this.mongoCDCInputConfig);

            mongoCDCRecords.add(mongoCDCRecord);
            dataTran.appendData(new CommonData(mongoCDCRecords));
        }
    }
    public void start( ){
        changeStreamIterable = openChangeStream(   );
        if (mongoCDCInputConfig.isUpdateLookup()) {
            changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP);
        }
        if (mongoCDCInputConfig.isIncludePreImage()) {
            changeStreamIterable.fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE);
        }
        if(mongoCDCInputConfig.isEnableIncrement()) {
            Status status = dataTran.getCurrentStatus();
            if (status.getCurrentLastValueWrapper() != null && status.getCurrentLastValueWrapper().getStrLastValue() != null) {
                String position = status.getCurrentLastValueWrapper().getStrLastValue();
                logger.info("Resuming streaming from token '{}'", position);

                final BsonDocument doc = new BsonDocument();
                doc.put("_data", new BsonString(position));
                changeStreamIterable.resumeAfter(doc);
            } else if (mongoCDCInputConfig.getLastTimeStamp() != null) {
                logger.info("Resuming streaming from operation time '{}'", mongoCDCInputConfig.getLastTimeStamp());
                changeStreamIterable.startAtOperationTime(new BsonTimestamp(mongoCDCInputConfig.getLastTimeStamp()));
            }
        }

        if (mongoCDCInputConfig.getCursorMaxAwaitTime() > 0) {
            changeStreamIterable.maxAwaitTime(mongoCDCInputConfig.getCursorMaxAwaitTime(), TimeUnit.MILLISECONDS);
        }
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStreamIterable.cursor();
        statusRunning = true;
//        int live = 0;
        while(true) {
            synchronized (lock) {
                if (!statusRunning)
                    break;
            }

            // Use tryNext which will return null if no document is yet available from the cursor.
            // In this situation if not document is available, we'll pause.
            try {
                final ChangeStreamDocument<Document> event = cursor.tryNext();
                if (event != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Arrived Change Stream event: {}", event);
                    }
//                    live++;
//                    if (live >= 10)
//                        throw new Exception("test exception stop job.");

                    dipatcheData(event);

                } else {
                    // No event was returned, so trigger a heartbeat

                    // Guard against `null` to be protective of issues like SERVER-63772, and situations called out in the Javadocs:
                    // > resume token [...] can be null if the cursor has either not been iterated yet, or the cursor is closed.
                    if (cursor.getResumeToken() != null) {
//                            rsOffsetContext.noEvent(cursor);
//                            dispatcher.dispatchHeartbeatEvent(rsPartition, rsOffsetContext);
                        sendDropedEvent(getPosition(cursor.getResumeToken()));
                    }


                }
            }
            catch (InterruptedException e) {
                logger.warn("Shutdown MongoDBChangeStreamListener[] on InterruptedException");
                shutdown();
                break;
            }
            catch (Exception e){
//                        logger.error("处理数据异常：",e);
                importContext.getDataTranPlugin().throwException(dataTran.getTaskContext(),e);
                if(!importContext.isContinueOnError()){
                    shutdown();
                    importContext.shutDownJob(e,true,false);
                }
            }
            catch (Throwable e){
//                        logger.error("处理数据异常：",e);
                importContext.getDataTranPlugin().throwException(dataTran.getTaskContext(),e);
                if(!importContext.isContinueOnError()){
                    shutdown();
                    importContext.shutDownJob(e,true,false);
                }
            }

        }

    }

    private void sendDropedEvent(String postion) throws InterruptedException {
        if(!isIncreament)
            return ;

        if(lastDroppedPostion != null && lastDroppedPostion.equals(postion))
            return;
        lastDroppedPostion = postion;
        MongoDBCDCData mongoDBCDCData = new MongoDBCDCData();
        mongoDBCDCData.setClusterTime(System.nanoTime());
        mongoDBCDCData.setPosition(postion);
        mongoDBCDCData.setAction(Record.RECORD_DIRECT_IGNORE);
        List<MongoCDCRecord> mongoCDCRecords = new ArrayList<>(1);
        MongoCDCRecord mongoCDCRecord = new MongoCDCRecord(dataTran.getTaskContext(),importContext,
                mongoDBCDCData, this.mongoCDCInputConfig);

        mongoCDCRecords.add(mongoCDCRecord);
        dataTran.appendData(new CommonData(mongoCDCRecords));
    }

    private MongoDBCDCData buildDropedEvent(ChangeStreamDocument<Document> event) throws InterruptedException {
        if(!isIncreament)
            return null;

        MongoDBCDCData mongoDBCDCData = new MongoDBCDCData();
        mongoDBCDCData.setAction(Record.RECORD_DIRECT_IGNORE);

        return mongoDBCDCData;
    }

    /**
     * Opens change stream based on
     *
     * @return change stream iterable
     */
    private ChangeStreamIterable<Document> openChangeStream( ) {
        MongoDB mogodb = MongoDBHelper.getMongoDB(mongoCDCInputConfig.getName());
        final ChangeStreamPipeline pipeline = new ChangeStreamPipelineFactory(mongoCDCInputConfig).create();

//        // capture scope is database
//        if (SimpleStringUtil.isNotEmpty(mongoCDCInputConfig.getDB()) ) {
//            logger.info("Change stream is restricted to '{}' database", mongoCDCInputConfig.getDB());
//            return mogodb.getMongoDatabase(mongoCDCInputConfig.getDB()).watch(pipeline.getStages(), Document.class);
//        }

        // capture scope is deployment
        return mogodb.getMongo().watch(pipeline.getStages(), Document.class);
    }

    public void shutdown(){
        if(!statusRunning)
            return;
        synchronized (lock){
            statusRunning = false;
        }

    }

}
