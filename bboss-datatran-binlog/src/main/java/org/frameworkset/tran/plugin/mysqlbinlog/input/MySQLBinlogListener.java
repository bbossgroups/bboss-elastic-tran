package org.frameworkset.tran.plugin.mysqlbinlog.input;
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
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.record.CommonData;
import org.frameworkset.tran.status.LastValueWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * <p>Description: binlog监听器</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/4/3
 * @author biaoping.yin
 * @version 1.0
 */
public class MySQLBinlogListener {
    private static final Logger logger = LoggerFactory.getLogger(MySQLBinlogListener.class);
    private MySQLBinlogConfig mySQLBinlogConfig;
    private BaseDataTran mysqlBinlogDataTran;
    private ImportContext importContext;
    private MysqlBinlogInputDatatranPlugin mysqlBinlogInputDatatranPlugin;
    private BinaryLogClient client;
    private com.github.shyiko.mysql.binlog.GtidSet gtidSet;
    private ClientConnectThread clientConnectThread;
    private BinaryLogClient.EventListener binaryLogClientEventListener;
    private String lastGtid;
    private boolean enableGtidMode;
    /**
     * 当前采集位置
     */
    private Long position ;
    /**
     * 当前采集binlog日志文件
     */
    private String binlogFile;
    private boolean isIncreament;
    private List<BinFileInfo> binFileInfos;

    public MySQLBinlogListener(BaseDataTran mysqlBinlogDataTran, MySQLBinlogConfig mySQLBinlogConfig, ImportContext importContext) {
        this.mysqlBinlogDataTran = mysqlBinlogDataTran;
        this.mySQLBinlogConfig = mySQLBinlogConfig;
        this.importContext = importContext;
        this.mysqlBinlogInputDatatranPlugin = (MysqlBinlogInputDatatranPlugin) importContext.getInputPlugin();
        isIncreament = mysqlBinlogDataTran.getDataTranPlugin().isIncreamentImport();
        if(mySQLBinlogConfig.isCollectMasterHistoryBinlog()){

            binFileInfos = DBMetaUtil.availableBinlogFiles(mySQLBinlogConfig);
            if(binFileInfos != null) {
                for (BinFileInfo binFileInfo : binFileInfos) {
                    binFileInfo.setFileName(SimpleStringUtil.getPath(mySQLBinlogConfig.getBinlogDir(), binFileInfo.getFileName()));
                }
            }
            else{
                binFileInfos = new ArrayList<>();
            }
        }
        else if(mySQLBinlogConfig.getFileNames() != null){
            binFileInfos = mySQLBinlogConfig.getCollectorFileNames();
        }
        else if(isIncreament){

            if(mysqlBinlogDataTran.getDataTranPlugin().getCurrentStatus() != null) {
                LastValueWrapper lastValueWrapper = mysqlBinlogDataTran.getDataTranPlugin().getCurrentStatus().getCurrentLastValueWrapper();
                Object lastValue = lastValueWrapper.getLastValue();
                if (lastValue != null) {
                    String t = String.valueOf(lastValue);
                    position = Long.parseLong(t);
                }
                String binlogFile_ = lastValueWrapper.getStrLastValue();
                if(binlogFile_ != null) {
                    if (binlogFile_.indexOf(MySQLBinlogConfig.split) > 0) {
                        String data[] = binlogFile_.split(MySQLBinlogConfig.split);
                        lastGtid = data[0];
                        binlogFile = data[1];
                    } else {
                        binlogFile = binlogFile_;
                    }
                    if (binlogFile != null){
                        List<BinFileInfo> logFiles = DBMetaUtil.availableBinlogFiles(mySQLBinlogConfig);
                        if(!DBMetaUtil.isBinlogAvailable(mySQLBinlogConfig,this,logFiles)){
                            binlogFile = null;
                            position = null;
                            lastGtid = null;
                        }
                    }
                }
                else{
//                    if(mySQLBinlogConfig.isIncreamentFrom001()){
//                        List<BinFileInfo> logFiles = DBMetaUtil.availableBinlogFiles(mySQLBinlogConfig);
//                        if(logFiles.size() > 0) {
//                            binlogFile = logFiles.get(0).getFileName();
//                            position = 0L;
//                        }
//                    }
                }

            }
            else if(mySQLBinlogConfig.getPosition() != null){
                position = mySQLBinlogConfig.getPosition();
                binlogFile = mySQLBinlogConfig.getMastterBinLogFile();
                this.lastGtid = mySQLBinlogConfig.getGtId();
                if (binlogFile != null){
                    List<BinFileInfo> logFiles = DBMetaUtil.availableBinlogFiles(mySQLBinlogConfig);
                    if(!DBMetaUtil.isBinlogAvailable(mySQLBinlogConfig,this,logFiles)){
                        binlogFile = null;
                        position = null;
                        lastGtid = null;
                    }
                }
            }
            else{
//                if(mySQLBinlogConfig.isIncreamentFrom001()){
//                    List<BinFileInfo> logFiles = DBMetaUtil.availableBinlogFiles(mySQLBinlogConfig);
//                    if(logFiles.size() > 0) {
//                        binlogFile = logFiles.get(0).getFileName();
//                        position = 0L;
//                    }
//                }
            }

        }

    }

    public void setBinlogFile(String binlogFile) {
        this.binlogFile = binlogFile;
    }

    public void setLastGtid(String lastGtid) {
        this.lastGtid = lastGtid;
    }

    public void setPosition(Long position) {
        this.position = position;
    }

    public Long getPosition() {
        return position;
    }

    public String getBinlogFile() {
        return binlogFile;
    }

    public GtidSet getGtidSet() {
        return gtidSet;
    }

    public String getLastGtid() {
        return lastGtid;
    }

    public void start() throws IOException {
        if(binFileInfos != null){
            listBinLogFiles();
        }
        else{
            listMasterSlave();
        }
    }



    public void listMasterSlave(){
        BinaryLogClient client = new BinaryLogClient(mySQLBinlogConfig.getHost(), mySQLBinlogConfig.getPort(),
                            mySQLBinlogConfig.getDbUser(), mySQLBinlogConfig.getDbPassword());
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
//                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        if(SimpleStringUtil.isNotEmpty(binlogFile)) {

            client.setBinlogFilename(binlogFile);
        }
        if (position != null)
            client.setBinlogPosition(position);
        client.setEventDeserializer(eventDeserializer);
        if(mySQLBinlogConfig.getServerId() != null){
            client.setServerId(mySQLBinlogConfig.getServerId());
        }

        if(mySQLBinlogConfig.getKeepAlive() != null){
            client.setKeepAlive(mySQLBinlogConfig.getKeepAlive());
        }
        if(mySQLBinlogConfig.getKeepAliveInterval() != null){
            client.setKeepAliveInterval(mySQLBinlogConfig.getKeepAliveInterval());
        }

        if(mySQLBinlogConfig.getHeartbeatInterval() != null){
            client.setHeartbeatInterval(mySQLBinlogConfig.getHeartbeatInterval());
        }
        if(mySQLBinlogConfig.getBlocking() != null){
            client.setBlocking(mySQLBinlogConfig.getBlocking());
        }

        if(mySQLBinlogConfig.getConnectTimeout() != null){
            client.setConnectTimeout(mySQLBinlogConfig.getConnectTimeout());
        }
        if(mySQLBinlogConfig.getGtidSetFallbackToPurged() != null){
            client.setGtidSetFallbackToPurged(mySQLBinlogConfig.getGtidSetFallbackToPurged());
        }
        if(mySQLBinlogConfig.getUseBinlogFilenamePositionInGtidMode() != null){
            client.setUseBinlogFilenamePositionInGtidMode(mySQLBinlogConfig.getUseBinlogFilenamePositionInGtidMode());
        }
        if(mySQLBinlogConfig.getSslMode() != null){
            client.setSSLMode(mySQLBinlogConfig.getSslMode());
        }
        if(mySQLBinlogConfig.getSslSocketFactory() != null){
            client.setSslSocketFactory(mySQLBinlogConfig.getSslSocketFactory());
        }

        client.registerEventListener(binaryLogClientEventListener = new BinaryLogClient.EventListener() {
            private String table = null;
            private String database = null;
            private byte[] columnTypes = null;
            private int[] columnMetadata = null;
            private String[] columns = null;
            private boolean containTable = false;
            private String binLogFileName;
            private long binlogPosition;
            private void sendDropedEvent() throws InterruptedException {
                if(!isIncreament)
                    return;
                List<MysqlBinlogRecord> datas = new ArrayList<>(1);

                MysqlBinLogData mysqlBinLogData = new MysqlBinLogData();
                mysqlBinLogData.setFileName(binLogFileName);
                mysqlBinLogData.setPosition(binlogPosition);
                mysqlBinLogData.setAction(Record.RECORD_DIRECT_IGNORE);

                MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(),mysqlBinLogData,mySQLBinlogConfig);
                datas.add(mysqlBinlogRecord);

                mysqlBinlogDataTran.appendData(new CommonData(datas));
            }
            @Override
            public void onEvent(Event event) {
                try {
                    binlogPosition = client.getBinlogPosition();
                    binLogFileName = client.getBinlogFilename();
                    EventData eventData = event.getData();
                    if(eventData != null) {
                        if (eventData instanceof RotateEventData) {
                            RotateEventData rotateEventData = (RotateEventData) eventData;
                            binLogFileName = rotateEventData.getBinlogFilename();

                            sendDropedEvent();
                        } else if (eventData instanceof TableMapEventData) {
                            TableMapEventData tableMapEventData = (TableMapEventData) event.getData();
                            table = tableMapEventData.getTable();
                            database = tableMapEventData.getDatabase();

                            columns = mysqlBinlogInputDatatranPlugin.getColumns(database, table);
                            if (columns != null) {
                                containTable = true;
                                columnTypes = tableMapEventData.getColumnTypes();
                                columnMetadata = tableMapEventData.getColumnMetadata();
                            } else {
                                containTable = false;
                                columnTypes = null;
                                columnMetadata = null;
                            }


                        } else if (eventData instanceof WriteRowsEventData) {
                            if (!containTable) {
                                sendDropedEvent();
                                return;
                            }
                            WriteRowsEventData writeRowsEventData = (WriteRowsEventData) event.getData();
                            List<Serializable[]> rows = writeRowsEventData.getRows();
                            List<MysqlBinlogRecord> datas = new ArrayList<>();
                            for (Serializable[] serializables : rows) {
                                Map<String, Object> row = converRow(columns, serializables, columnTypes,
                                        columnMetadata);
                                MysqlBinLogData mysqlBinLogData = new MysqlBinLogData();
                                mysqlBinLogData.setData(row);
                                mysqlBinLogData.setTable(table);
                                mysqlBinLogData.setDatabase(database);
                                mysqlBinLogData.setFileName(binLogFileName);
                                mysqlBinLogData.setPosition(binlogPosition);
                                mysqlBinLogData.setAction(Record.RECORD_INSERT);

                                MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(), mysqlBinLogData, mySQLBinlogConfig);
                                datas.add(mysqlBinlogRecord);

                            }
                            mysqlBinlogDataTran.appendData(new CommonData(datas));

                        } else if (eventData instanceof UpdateRowsEventData) {
                            if (!containTable) {
                                sendDropedEvent();
                                return;
                            }
                            UpdateRowsEventData writeRowsEventData = (UpdateRowsEventData) event.getData();
                            List<Map.Entry<Serializable[], Serializable[]>> rows = writeRowsEventData.getRows();
                            List<MysqlBinlogRecord> datas = new ArrayList<>();
                            for (Map.Entry<Serializable[], Serializable[]> serializables : rows) {
                                Map<String, Object> oldrow = converRow(columns, serializables.getKey(), columnTypes,
                                        columnMetadata);

                                Map<String, Object> newrow = converRow(columns, serializables.getValue(), columnTypes,
                                        columnMetadata);
                                MysqlBinLogData mysqlBinLogData = new MysqlBinLogData();
                                mysqlBinLogData.setData(newrow);
                                mysqlBinLogData.setOldValues(oldrow);
                                mysqlBinLogData.setTable(table);
                                mysqlBinLogData.setDatabase(database);
                                mysqlBinLogData.setAction(Record.RECORD_UPDATE);
                                mysqlBinLogData.setFileName(binLogFileName);
                                mysqlBinLogData.setPosition(binlogPosition);
                                MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(), mysqlBinLogData, mySQLBinlogConfig);
                                datas.add(mysqlBinlogRecord);

                            }
                            mysqlBinlogDataTran.appendData(new CommonData(datas));
//                    System.out.println(event);
                        } else if (eventData instanceof DeleteRowsEventData) {
                            if (!containTable) {
                                sendDropedEvent();
                                return;
                            }
                            DeleteRowsEventData writeRowsEventData = (DeleteRowsEventData) event.getData();
                            List<Serializable[]> rows = writeRowsEventData.getRows();
                            List<MysqlBinlogRecord> datas = new ArrayList<>();
                            for (Serializable[] serializables : rows) {
                                Map<String, Object> row = converRow(columns, serializables, columnTypes,
                                        columnMetadata);
                                MysqlBinLogData mysqlBinLogData = new MysqlBinLogData();
                                mysqlBinLogData.setData(row);
                                mysqlBinLogData.setTable(table);
                                mysqlBinLogData.setDatabase(database);
                                mysqlBinLogData.setAction(Record.RECORD_DELETE);
                                mysqlBinLogData.setFileName(binLogFileName);
                                mysqlBinLogData.setPosition(binlogPosition);
                                MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(), mysqlBinLogData, mySQLBinlogConfig);
                                datas.add(mysqlBinlogRecord);

                            }
                            mysqlBinlogDataTran.appendData(new CommonData(datas));
                        }
                        else if (eventData instanceof QueryEventData) {
                            if(mySQLBinlogConfig.isDdlSyn()){
                                QueryEventData queryEventData = (QueryEventData)eventData;
                                String db = queryEventData.getDatabase();
                                if(isDdlSynDatabases(db)) {
                                    List<MysqlBinlogRecord> datas = new ArrayList<>();
                                    MysqlBinLogData mysqlBinLogData = new MysqlBinLogData();
                                    Map<String, Object> data = new LinkedHashMap<>();

                                    data.put("ddl", queryEventData.getSql());
                                    data.put("errorCode", queryEventData.getErrorCode());
                                    data.put("executionTime", queryEventData.getExecutionTime());

                                    mysqlBinLogData.setData(data);
//                                mysqlBinLogData.setTable(table);
                                    mysqlBinLogData.setDatabase(db);
                                    mysqlBinLogData.setAction(Record.RECORD_DDL);
                                    mysqlBinLogData.setFileName(binLogFileName);
                                    mysqlBinLogData.setPosition(binlogPosition);
                                    MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(), mysqlBinLogData, mySQLBinlogConfig);
                                    datas.add(mysqlBinlogRecord);
                                    mysqlBinlogDataTran.appendData(new CommonData(datas));
                                }
                            }
                            else{
                                sendDropedEvent();
                            }
                        }
                        else {
                            sendDropedEvent();
                        }
                    }
                    else{
                        sendDropedEvent();
                    }
                } catch (IOException e) {
                    logger.error("处理数据异常：",e);
                    importContext.getDataTranPlugin().throwException(mysqlBinlogDataTran.getTaskContext(),e);
                } catch (InterruptedException e) {
                    logger.warn("Shutdown MySQLBinlogListener[] on InterruptedException");
                    shutdown();
                }
            }

        });

        this.client = client;
        ClientConnectThread clientConnectThread = new ClientConnectThread(client);
        if(mySQLBinlogConfig.getJoinToConnectTimeOut() > 0L) {

            clientConnectThread.start();
            this.clientConnectThread = clientConnectThread;
            try {

                clientConnectThread.join(mySQLBinlogConfig.getJoinToConnectTimeOut());


            } catch (InterruptedException e) {

            }
            if (clientConnectThread.getDataImportException() != null) {
                throw clientConnectThread.getDataImportException();
            }
        }
        else{
            clientConnectThread.run();
        }


    }





    public void listBinLogFiles(){
        long start = System.currentTimeMillis();

        for(BinFileInfo fileName:this.binFileInfos){
            try {
                listBinLogFile(fileName);
            } catch (InterruptedException e) {
                logger.warn("Shutdown MySQLBinlogListener[{}] on InterruptedException",fileName);
                shutdown();
                break;
            }
        }

        try {
            List<MysqlBinlogRecord> datas = new ArrayList<>();
            datas.add(new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(),mySQLBinlogConfig,true,true,true));
            this.mysqlBinlogDataTran.appendData(new CommonData(datas));
            long end = System.currentTimeMillis();
            if(!mySQLBinlogConfig.isCollectMasterHistoryBinlog())
                logger.info("Collect binlogs[{}] finished.elapsed time：{}秒.",mySQLBinlogConfig.getFileNames(),(end -start)/1000);
            else{
                logger.info("Collect binlogs[{}] finished.elapsed time：{}秒.",binFileInfos,(end -start)/1000);
            }
        } catch (InterruptedException e) {
            shutdown();
        }
    }

    public void listBinLogFile(BinFileInfo fileName) throws InterruptedException {


        BinaryLogFileReader reader = null;

        long start = System.currentTimeMillis();
        try {
            logger.info("Collect binlog["+fileName+"] begin.");
            File binlogFile = new File(fileName.getFileName());
            EventDeserializer eventDeserializer = new EventDeserializer();
            eventDeserializer.setCompatibilityMode(
//                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                    EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
            );
            reader = new BinaryLogFileReader(binlogFile, eventDeserializer);
            String table = null;
            String database = null;
            TableMapEventData tableMapEventData = null;
            byte[] columnTypes = null;
            int[] columnMetadata = null;
            String[] columns = null;
            boolean containTable = false;
            Map<String,TableMapEventData> tableMapEventDataMap = new LinkedHashMap<>();
            for (Event event; (event = reader.readEvent()) != null; ) {
                EventData eventData = event.getData();
                if(eventData instanceof TableMapEventData) {
                    tableMapEventData = (TableMapEventData)event.getData();
                     table = tableMapEventData.getTable();
                     database = tableMapEventData.getDatabase();

                    columns = this.mysqlBinlogInputDatatranPlugin.getColumns(database,table);
                    if(columns != null) {
                        containTable = true;
                        columnTypes = tableMapEventData.getColumnTypes();
                        columnMetadata = tableMapEventData.getColumnMetadata();
                        tableMapEventDataMap.put(tableMapEventData.getTableId()+"",tableMapEventData);
                    }
                    else{
                        containTable = false;
                        columnTypes = null;
                        columnMetadata = null;
                    }



                } else if(eventData instanceof WriteRowsEventData) {
                    if(!containTable){
                        continue;
                    }
                    WriteRowsEventData writeRowsEventData = (WriteRowsEventData)event.getData();
                    List<Serializable[]> rows = writeRowsEventData.getRows();
                    List<MysqlBinlogRecord> datas = new ArrayList<>();
                    for(Serializable[] serializables:rows){
                        Map<String,Object> row = converRow( columns,serializables, columnTypes,
                         columnMetadata );
                        MysqlBinLogData mysqlBinLogData = new MysqlBinLogData();
                        mysqlBinLogData.setData(row);
                        mysqlBinLogData.setTable(table);
                        mysqlBinLogData.setDatabase(database);
                        mysqlBinLogData.setAction(Record.RECORD_INSERT);
                        mysqlBinLogData.setFileName(fileName.getFileName());
                        MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(),mysqlBinLogData,mySQLBinlogConfig);
                        datas.add(mysqlBinlogRecord);

                    }
                    this.mysqlBinlogDataTran.appendData(new CommonData(datas));

                }
                else if(eventData instanceof UpdateRowsEventData) {
                    if(!containTable){
                        continue;
                    }
                    UpdateRowsEventData writeRowsEventData = (UpdateRowsEventData)event.getData();
                    List<Map.Entry<Serializable[], Serializable[]>> rows = writeRowsEventData.getRows();
                    List<MysqlBinlogRecord> datas = new ArrayList<>();
                    for(Map.Entry<Serializable[], Serializable[]> serializables:rows){
                        Map<String,Object> oldrow = converRow( columns,serializables.getKey(), columnTypes,
                                columnMetadata );

                        Map<String,Object> newrow = converRow( columns,serializables.getValue(), columnTypes,
                                columnMetadata );
                        MysqlBinLogData mysqlBinLogData = new MysqlBinLogData();
                        mysqlBinLogData.setData(newrow);
                        mysqlBinLogData.setOldValues(oldrow);
                        mysqlBinLogData.setTable(table);
                        mysqlBinLogData.setDatabase(database);
                        mysqlBinLogData.setAction(Record.RECORD_UPDATE);
                        mysqlBinLogData.setFileName(fileName.getFileName());
                        MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(),mysqlBinLogData,mySQLBinlogConfig);
                        datas.add(mysqlBinlogRecord);

                    }
                    this.mysqlBinlogDataTran.appendData(new CommonData(datas));
//                    System.out.println(event);
                }
                else if(eventData instanceof DeleteRowsEventData) {
                    if(!containTable){
                        continue;
                    }
                    DeleteRowsEventData writeRowsEventData = (DeleteRowsEventData)event.getData();
                    List<Serializable[]> rows = writeRowsEventData.getRows();
                    List<MysqlBinlogRecord> datas = new ArrayList<>();
                    for(Serializable[] serializables:rows){
                        Map<String,Object> row = converRow( columns,serializables, columnTypes,
                                columnMetadata );
                        MysqlBinLogData mysqlBinLogData = new MysqlBinLogData();
                        mysqlBinLogData.setData(row);
                        mysqlBinLogData.setTable(table);
                        mysqlBinLogData.setDatabase(database);
                        mysqlBinLogData.setAction(Record.RECORD_DELETE);
                        mysqlBinLogData.setFileName(fileName.getFileName());
                        MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(),mysqlBinLogData,mySQLBinlogConfig);
                        datas.add(mysqlBinlogRecord);

                    }
                    this.mysqlBinlogDataTran.appendData(new CommonData(datas));
                }
                else if (eventData instanceof QueryEventData) {
                    if(mySQLBinlogConfig.isDdlSyn()){
                        QueryEventData queryEventData = (QueryEventData)eventData;
                        String db = queryEventData.getDatabase();
                        if(isDdlSynDatabases(db)) {
                            List<MysqlBinlogRecord> datas = new ArrayList<>();
                            MysqlBinLogData mysqlBinLogData = new MysqlBinLogData();
                            Map<String, Object> data = new LinkedHashMap<>();

                            data.put("ddl", queryEventData.getSql());
                            data.put("errorCode", queryEventData.getErrorCode());
                            data.put("executionTime", queryEventData.getExecutionTime());
                            mysqlBinLogData.setData(data);
                            mysqlBinLogData.setDatabase(db);
                            mysqlBinLogData.setAction(Record.RECORD_DDL);
                            mysqlBinLogData.setFileName(fileName.getFileName());
                            MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(), mysqlBinLogData, mySQLBinlogConfig);
                            datas.add(mysqlBinlogRecord);
                            mysqlBinlogDataTran.appendData(new CommonData(datas));
                        }
                    }

                }
            }

            long end = System.currentTimeMillis();
            logger.info("Collect binlog[{}] finished. Elapsed time：{}秒.",fileName,(end -start)/1000);
        } catch (InterruptedException e) {
//            throw new RuntimeException(e);

            throw e;

        }
        catch (IOException e) {
            logger.error("处理数据IO异常：",e);
            importContext.getDataTranPlugin().throwException(mysqlBinlogDataTran.getTaskContext(),e);
        }
        catch (Exception e) {
            logger.error("处理数据异常：",e);
            importContext.getDataTranPlugin().throwException(mysqlBinlogDataTran.getTaskContext(),e);
        }
        catch (Throwable e) {
            logger.error("处理数据异常：",e);
            importContext.getDataTranPlugin().throwException(mysqlBinlogDataTran.getTaskContext(),e);
        }
        finally {
            try {
                reader.close();
            } catch (IOException e) {
                logger.warn("Close reader["+binFileInfos+"] failed:",e);
            }


        }

    }

    private boolean isDdlSynDatabases(String database){
        return mySQLBinlogConfig.isDdlSynDatabases(database);
    }


    public void shutdown() {
        if(client != null){
            try{
                if(binaryLogClientEventListener != null)
                    client.unregisterEventListener(binaryLogClientEventListener);
            } catch (Exception e) {
                logger.error("",e);
            }
            try {

                client.disconnect();
            } catch (Exception e) {
                logger.error("",e);
            }
            if(clientConnectThread != null){
                try {
                    clientConnectThread.join(5000L);
                }
                catch (Exception interruptedException){

                }
            }
        }



    }

    private Map<String,Object> converRow(String columns[],Serializable[] data, byte[] columnTypes,
                                         int[] columnMetadata ) throws IOException {

        Map<String, Object> b = new HashMap<>(data.length);
        for (int i = 0; i < data.length; i++) {
            int typeCode = columnTypes[i] & 0xFF, meta = columnMetadata[i], length = 0;
            if (typeCode == ColumnType.STRING.getCode()) {
                if (meta >= 256) {
                    int meta0 = meta >> 8, meta1 = meta & 0xFF;
                    if ((meta0 & 0x30) != 0x30) {
                        typeCode = meta0 | 0x30;
                        length = meta1 | (((meta0 & 0x30) ^ 0x30) << 4);
                    } else {
                        // mysql-5.6.24 sql/rpl_utility.h enum_field_types (line 278)
                        if (meta0 == ColumnType.ENUM.getCode() || meta0 == ColumnType.SET.getCode()) {
                            typeCode = meta0;
                        }
                        length = meta1;
                    }
                } else {
                    length = meta;
                }
            }
            Object value = null;
            ColumnType columnType = ColumnType.byCode(typeCode);
            switch (columnType) {
                case BIT://BitSet
                    value = data[i];
                    break;
                case TINY://int
                    value = data[i];
                    break;
                case SHORT://int
                    value = data[i];
                    break;
                case INT24://int
                    value = data[i];
                    break;
                case LONG://int
                    value = data[i];
                    break;
                case LONGLONG://long
                    value = data[i];
                    break;
                case FLOAT://float
                    value = data[i];
                    break;
                case DOUBLE://double
                    value = data[i];
                    break;
                case NEWDECIMAL://BigDecimal
                    value = data[i];
                    break;
                case DATE://java.sql.Date
                    value = data[i];
                    break;
                case TIME://java.sql.Time
                    value = data[i];
                    break;
                case TIME_V2://java.sql.Time
                    value = data[i];
                    break;
                case TIMESTAMP://java.sql.Timestamp
                    value = data[i];
                    break;
                case TIMESTAMP_V2://java.sql.Timestamp
                    value = data[i];
                    break;
                case DATETIME://java.util.Date
                    value = data[i];
                    break;
                case DATETIME_V2://java.util.Date
                    value = data[i];
                    break;
                case YEAR://int
                    value = data[i];
                    break;
                case STRING: // byte[]
                    Object t = data[i];
                    if(t != null) {
                        value = new String((byte[]) t, StandardCharsets.UTF_8);
                    }

                    break;
                case VARCHAR:
                case VAR_STRING: //byte[] VARCHAR or VARBINARY
                    Object tt = data[i];
                    if(tt != null) {
                        value = new String((byte[]) tt, StandardCharsets.UTF_8);
                    }

                    break;
                case BLOB:// byte[]
                    value = data[i];
                    break;
                case ENUM://int
                    value = data[i];
                    break;
                case SET://long
                    value = data[i];
                    break;
                case GEOMETRY://byte[]
                    value = data[i];
                    break;
                case JSON:
                    Object ttt = data[i];
                    if(ttt != null) {
                        value = JsonBinary.parseAsString((byte[]) ttt);
                    }

                    break;
                default:
                    throw new IOException("Unsupported type " + columnType);
            }
            b.put(columns[i], value);
        }
        return b;
    }

    public boolean isEnableGtidMode() {
        return enableGtidMode;
    }

    public void setEnableGtidMode(boolean enableGtidMode) {
        this.enableGtidMode = enableGtidMode;
    }
}
