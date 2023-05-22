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

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.record.CommonData;
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
    private Long position = null;
    public MySQLBinlogListener(BaseDataTran mysqlBinlogDataTran, MySQLBinlogConfig mySQLBinlogConfig, ImportContext importContext) {
        this.mysqlBinlogDataTran = mysqlBinlogDataTran;
        this.mySQLBinlogConfig = mySQLBinlogConfig;
        this.importContext = importContext;
        this.mysqlBinlogInputDatatranPlugin = (MysqlBinlogInputDatatranPlugin) importContext.getInputPlugin();
        if(mysqlBinlogDataTran.getDataTranPlugin().isIncreamentImport()){
            if(mysqlBinlogDataTran.getDataTranPlugin().getCurrentStatus() != null) {
                Object lastValue = mysqlBinlogDataTran.getDataTranPlugin().getCurrentStatus().getLastValue();
                if (lastValue != null) {
                    String t = String.valueOf(lastValue);
                    position = Long.parseLong(t);
                }
            }
            else if(mySQLBinlogConfig.getPosition() != null){
                position = mySQLBinlogConfig.getPosition();
            }
        }
        else if(mySQLBinlogConfig.getPosition() != null){
            position = mySQLBinlogConfig.getPosition();
        }
    }

    public void start() throws IOException {
        if(mySQLBinlogConfig.getFileNames() != null){
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
        if(position != null)
            client.setBinlogPosition(position);
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(new BinaryLogClient.EventListener() {
            private String table = null;
            private String database = null;
            private byte[] columnTypes = null;
            private int[] columnMetadata = null;
            private String[] columns = null;
            private boolean containTable = false;
            @Override
            public void onEvent(Event event) {
                try {
                    long currentPosition = client.getBinlogPosition();
                    EventData eventData = event.getData();
                    if (eventData instanceof TableMapEventData) {
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
                            mysqlBinLogData.setPosition(currentPosition);
                            mysqlBinLogData.setAction(Record.RECORD_INSERT);
                            MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(),mysqlBinLogData,mySQLBinlogConfig);
                            datas.add(mysqlBinlogRecord);

                        }
                        mysqlBinlogDataTran.appendData(new CommonData(datas));

                    } else if (eventData instanceof UpdateRowsEventData) {
                        if (!containTable) {
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
                            mysqlBinLogData.setAction(Record.RECORD_UPDATE);
                            mysqlBinLogData.setPosition(currentPosition);
                            MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(),mysqlBinLogData,mySQLBinlogConfig);
                            datas.add(mysqlBinlogRecord);

                        }
                        mysqlBinlogDataTran.appendData(new CommonData(datas));
//                    System.out.println(event);
                    } else if (eventData instanceof DeleteRowsEventData) {
                        if (!containTable) {
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
                            mysqlBinLogData.setAction(Record.RECORD_DELETE);
                            mysqlBinLogData.setPosition(currentPosition);
                            MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(),mysqlBinLogData,mySQLBinlogConfig);
                            datas.add(mysqlBinlogRecord);

                        }
                        mysqlBinlogDataTran.appendData(new CommonData(datas));
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
        try {
            client.connect();
            this.client = client;
        } catch (IOException e) {
            throw new DataImportException(e);
        }
    }



    public void listBinLogFiles(){
        long start = System.currentTimeMillis();
        for(String fileName:mySQLBinlogConfig.getCollectorFileNames()){
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
            logger.info("Collect binlogs[{}] finished.elapsed time：{}秒.",mySQLBinlogConfig.getFileNames(),(end -start)/1000);
        } catch (InterruptedException e) {
            shutdown();
        }
    }

    public void listBinLogFile(String fileName) throws InterruptedException {


        BinaryLogFileReader reader = null;

        long start = System.currentTimeMillis();
        try {
            logger.info("Collect binlog["+fileName+"] begin.");
            File binlogFile = new File(fileName);
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
                        mysqlBinLogData.setAction(Record.RECORD_INSERT);
                        mysqlBinLogData.setFileName(fileName);
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
                        mysqlBinLogData.setAction(Record.RECORD_UPDATE);
                        mysqlBinLogData.setFileName(fileName);
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
                        mysqlBinLogData.setAction(Record.RECORD_DELETE);
                        mysqlBinLogData.setFileName(fileName);
                        MysqlBinlogRecord mysqlBinlogRecord = new MysqlBinlogRecord(mysqlBinlogDataTran.getTaskContext(),mysqlBinLogData,mySQLBinlogConfig);
                        datas.add(mysqlBinlogRecord);

                    }
                    this.mysqlBinlogDataTran.appendData(new CommonData(datas));
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
                logger.warn("Close reader["+mySQLBinlogConfig.getFileNames()+"] failed:",e);
            }


        }

    }


    public void shutdown() {
        if(client != null){
            try {
                client.disconnect();
            } catch (IOException e) {
                logger.error("",e);
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
}
