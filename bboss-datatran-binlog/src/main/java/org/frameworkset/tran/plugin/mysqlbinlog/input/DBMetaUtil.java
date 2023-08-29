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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.plugin.db.CDCDBTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/5/17
 * @author biaoping.yin
 * @version 1.0
 */
public class DBMetaUtil {
    private static Logger logger = LoggerFactory.getLogger(DBMetaUtil.class);
//    static {
//        try {
//            Class.forName("com.mysql.jdbc.Driver");
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
//    }

    public static String buildTableKey(String database,String table){
        return new StringBuilder().append(database).append(":").append(table).toString();
    }
    public static void initTableColumns(MySQLBinlogConfig mySQLBinlogConfig){
        Map<String,String[]> allTableColumns = new LinkedHashMap<>();
        Map<String, List<CDCDBTable>> dbTables = mySQLBinlogConfig.getDbTables();
        if(dbTables != null && dbTables.size() > 0) {
            Iterator<Map.Entry<String, List<CDCDBTable>>> iterator = dbTables.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, List<CDCDBTable>> entry = iterator.next();
                List<CDCDBTable> tables = entry.getValue();
                for(CDCDBTable cdcdbTable: tables){
                    List<String> tableColums = getColumns(mySQLBinlogConfig, entry.getKey(),cdcdbTable.getTableName());
                    cdcdbTable.setTableColumns(tableColums.toArray(new String[tableColums.size()]));
                }

            }

        }
    }
    private static List<String> getColumns(MySQLBinlogConfig mySQLBinlogConfig,String database,  String table) {
        ResultSet resultSet =null;
        Statement statement = null;
        Connection connection =null;
        try {
//            String url =  "jdbc:mysql://" + host.getHost()+":" +host.getPort()+
//                    "/INFORMATION_SCHEMA?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
            connection  = DriverManager.getConnection(mySQLBinlogConfig.getSchemaUrl(),
                    mySQLBinlogConfig.getDbUser(), mySQLBinlogConfig.getDbPassword());
            statement = connection.createStatement();

            String sql = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='"
                    + database + "' and TABLE_NAME='" + table + "' order by ORDINAL_POSITION asc;";

            resultSet = statement.executeQuery(sql);
            List<String> buf = new ArrayList<>();

            while (resultSet.next()) {
                buf.add(resultSet.getString(1));
            }

            return buf;
        } catch (SQLException e) {
            throw new DataImportException(mySQLBinlogConfig.getSchemaUrl(),e);
        }finally {
            if(resultSet!=null){
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    logger.error("close reseutlSet error",e);
                }
            }

            if(statement!=null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("close statement error",e);
                }
            }
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("close connection error",e);
                }
            }
        }
    }
}
