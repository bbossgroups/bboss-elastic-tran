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

import com.frameworkset.common.poolman.DBUtil;
import com.frameworkset.common.poolman.sql.TableMetaData;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.frameworkset.tran.BaseDataTran;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: binlog监听器</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/4/3
 * @author biaoping.yin
 * @version 1.0
 */
public class MySQLBinlogListenerTest {
    private static final Logger logger = LoggerFactory.getLogger(MySQLBinlogListenerTest.class);
    private MySQLBinlogConfig mySQLBinlogConfig;
    private BaseDataTran mysqlBinlogDataTran;
    private BinaryLogClient client;
    public MySQLBinlogListenerTest(BaseDataTran mysqlBinlogDataTran, MySQLBinlogConfig mySQLBinlogConfig) {
        this.mysqlBinlogDataTran = mysqlBinlogDataTran;
        this.mySQLBinlogConfig = mySQLBinlogConfig;
    }



    public void listMasterSlave(){
        BinaryLogClient client = new BinaryLogClient(mySQLBinlogConfig.getHost(), mySQLBinlogConfig.getPort(),
                            mySQLBinlogConfig.getDbUser(), mySQLBinlogConfig.getDbPassword());
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(new BinaryLogClient.EventListener() {

            @Override
            public void onEvent(Event event) {

            }
        });
        try {
            client.connect();
            this.client = client;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void listBinLogFile() throws IOException {
        File binlogFile = new File(mySQLBinlogConfig.getFileNames());
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile, eventDeserializer);
        try {
            String table = null;
            String database = null;
            TableMapEventData tableMapEventData = null;
            byte[] columnTypes = null;
            int[] columnMetadata = null;
            int tableId = -1;
            TableMetaData tableMetaData = DBUtil.getTableMetaData("");
            Map<String,TableMapEventData> tableMapEventDataMap = new LinkedHashMap<>();
            for (Event event; (event = reader.readEvent()) != null; ) {
                EventData eventData = event.getData();
                if(eventData instanceof TableMapEventData) {
                    tableMapEventData = (TableMapEventData)event.getData();
                     table = tableMapEventData.getTable();
                     database = tableMapEventData.getDatabase();
                     tableMapEventData.getTableId();
                    columnTypes = tableMapEventData.getColumnTypes();
                    columnMetadata = tableMapEventData.getColumnMetadata();
                    System.out.println(event);
                    tableMapEventDataMap.put(tableMapEventData.getTableId()+"",tableMapEventData);
                } else if(eventData instanceof WriteRowsEventData) {
                    WriteRowsEventData writeRowsEventData = (WriteRowsEventData)event.getData();
                    BitSet includedColumns = writeRowsEventData.getIncludedColumns();
                    List<Serializable[]> rows = writeRowsEventData.getRows();

                    for(Serializable[] serializables:rows){
                        int i = 0;
                        for(Serializable serializable:serializables) {
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
                            ColumnType columnType = ColumnType.byCode(typeCode);

                            if (serializable instanceof byte[]) {
                                System.out.println(new String((byte[]) serializable, StandardCharsets.UTF_8));
                            }
                            else if (serializable instanceof Integer) {
                                System.out.println((Integer) serializable);
                            }
//                            switch (type) {
//                                case ColumnType.BIT:
//                                    return (BitSet)serializable;
//                                case ColumnType.TINY:
//                                    return deserializeTiny(inputStream);
//                                case ColumnType.SHORT:
//                                    return deserializeShort(inputStream);
//                                case ColumnType.INT24:
//                                    return deserializeInt24(inputStream);
//                                case ColumnType.LONG:
//                                    return deserializeLong(inputStream);
//                                case ColumnType.LONGLONG:
//                                    return deserializeLongLong(inputStream);
//                                case ColumnType.FLOAT:
//                                    return deserializeFloat(inputStream);
//                                case ColumnType.DOUBLE:
//                                    return deserializeDouble(inputStream);
//                                case ColumnType.NEWDECIMAL:
//                                    return deserializeNewDecimal(meta, inputStream);
//                                case ColumnType.DATE:
//                                    return deserializeDate(inputStream);
//                                case ColumnType.TIME:
//                                    return deserializeTime(inputStream);
//                                case ColumnType.TIME_V2:
//                                    return deserializeTimeV2(meta, inputStream);
//                                case ColumnType.TIMESTAMP:
//                                    return deserializeTimestamp(inputStream);
//                                case ColumnType.TIMESTAMP_V2:
//                                    return deserializeTimestampV2(meta, inputStream);
//                                case ColumnType.DATETIME:
//                                    return deserializeDatetime(inputStream);
//                                case ColumnType.DATETIME_V2:
//                                    return deserializeDatetimeV2(meta, inputStream);
//                                case ColumnType.YEAR:
//                                    return deserializeYear(inputStream);
//                                case ColumnType.STRING: // CHAR or BINARY
//                                    return deserializeString(length, inputStream);
//                                case ColumnType.VARCHAR:
//                                case ColumnType.VAR_STRING: // VARCHAR or VARBINARY
//                                    return deserializeVarString(meta, inputStream);
//                                case ColumnType.BLOB:
//                                    return deserializeBlob(meta, inputStream);
//                                case ColumnType.ENUM:
//                                    return deserializeEnum(length, inputStream);
//                                case ColumnType.SET:
//                                    return deserializeSet(length, inputStream);
//                                case ColumnType.GEOMETRY:
//                                    return deserializeGeometry(meta, inputStream);
//                                case ColumnType.JSON:
//                                    return deserializeJson(meta, inputStream);
//                                default:
//                                    throw new IOException("Unsupported type " + type);
//                            }
                            i ++;
                        }
                    }
//                    System.out.println(event);
                }
                else if(eventData instanceof UpdateRowsEventData) {
                    UpdateRowsEventData writeRowsEventData = (UpdateRowsEventData)event.getData();
                    writeRowsEventData.getIncludedColumns();
//                    System.out.println(event);
                }
                else if(eventData instanceof DeleteRowsEventData) {
                    DeleteRowsEventData writeRowsEventData = (DeleteRowsEventData)event.getData();
                    writeRowsEventData.getIncludedColumns();
//                    System.out.println(event);
                }
            }
        } finally {
            reader.close();
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
}
