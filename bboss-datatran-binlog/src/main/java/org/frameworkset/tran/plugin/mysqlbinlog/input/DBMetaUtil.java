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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    public static Map<String,String[]> getTableColumns(MySQLBinlogConfig mySQLBinlogConfig){
        Map<String,String[]> allTableColumns = new LinkedHashMap<>();
        for(String table:mySQLBinlogConfig.getListenTables()) {
            List<String> tableColums = getColumns(mySQLBinlogConfig,table);
            if(tableColums == null || tableColums.size() == 0){
                throw new DataImportException("getTableColumns failed:"+table + "'s columns is null.");
            }
            allTableColumns.put(buildTableKey( mySQLBinlogConfig.getDatabase(), table),tableColums.toArray(new String[tableColums.size()]));
        }
        return allTableColumns;
    }
    private static List<String> getColumns(MySQLBinlogConfig host,  String table) {
        ResultSet resultSet =null;
        Statement statement = null;
        Connection connection =null;
        try {
//            String url =  "jdbc:mysql://" + host.getHost()+":" +host.getPort()+
//                    "/INFORMATION_SCHEMA?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
            connection  = DriverManager.getConnection(host.getSchemaUrl(), host.getDbUser(), host.getDbPassword());
            statement = connection.createStatement();

            String sql = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='"
                    + host.getDatabase() + "' and TABLE_NAME='" + table + "' order by ORDINAL_POSITION asc;";

            resultSet = statement.executeQuery(sql);
            List<String> buf = new ArrayList<>();

            while (resultSet.next()) {
                buf.add(resultSet.getString(1));
            }

            return buf;
        } catch (SQLException e) {
            throw new DataImportException(host.getSchemaUrl(),e);
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
