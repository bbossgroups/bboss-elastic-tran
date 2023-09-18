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

import com.github.shyiko.mysql.binlog.GtidSet;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.plugin.db.CDCDBTable;
import org.frameworkset.tran.plugin.mysqlbinlog.input.util.BBossGtidSet;
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
        String sql = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='"
                + database + "' and TABLE_NAME='" + table + "' order by ORDINAL_POSITION asc;";
        return queryAndMap(mySQLBinlogConfig, sql, new ValueMap<List<String>>() {
            @Override
            public List<String> convert(ResultSet resultSet) throws Exception {
                List<String> buf = new ArrayList<>();

                while (resultSet.next()) {
                    buf.add(resultSet.getString(1));
                }

                return buf;
            }
        });

    }

    interface ValueMap<T>{
        public T convert(ResultSet rs) throws Exception;
    }

    public static  <T> T queryAndMap(MySQLBinlogConfig mySQLBinlogConfig,String sql,ValueMap<T> valueMap ){
        ResultSet resultSet =null;
        Statement statement = null;
        Connection connection =null;
        try {
//            String url =  "jdbc:mysql://" + host.getHost()+":" +host.getPort()+
//                    "/INFORMATION_SCHEMA?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
            connection  = DriverManager.getConnection(mySQLBinlogConfig.getSchemaUrl(),
                    mySQLBinlogConfig.getDbUser(), mySQLBinlogConfig.getDbPassword());
            statement = connection.createStatement();

//            String sql = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='"
//                    + database + "' and TABLE_NAME='" + table + "' order by ORDINAL_POSITION asc;";

            resultSet = statement.executeQuery(sql);
            return valueMap.convert(resultSet);

        } catch (Exception e) {
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
    /**
     * Determine whether the MySQL server has GTIDs enabled.
     *
     * @return {@code false} if the server's {@code gtid_mode} is set and is {@code OFF}, or {@code true} otherwise
     */
    public static boolean isGtidModeEnabled(MySQLBinlogConfig mySQLBinlogConfig) {
        try {
            return queryAndMap(mySQLBinlogConfig,"SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'", rs -> {
                if (rs.next()) {
                    return "ON".equalsIgnoreCase(rs.getString(2));
                }
                return false;
            });
        }
        catch (Exception e) {
            throw new DataImportException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }
    }

    /**
     * Determine the earliest binlog filename that is still available in the server.
     *
     * @return the name of the earliest binlog filename, or null if there are none.
     */
    public static String earliestBinlogFilename(MySQLBinlogConfig mySQLBinlogConfig) {
        // Accumulate the available binlog filenames ...
        List<String> logNames = null;
        try {
            logger.info("Checking all known binlogs from MySQL");
            logNames = queryAndMap(mySQLBinlogConfig,"SHOW BINARY LOGS", rs -> {
                List<String> logNames_ = new ArrayList<>();
                while (rs.next()) {
                    logNames_.add(rs.getString(1));
                }
                return logNames_;
            });
        }
        catch (Exception e) {
            throw new DataImportException("Unexpected error while connecting to MySQL and looking for binary logs: ", e);
        }

        if (logNames == null || logNames.isEmpty()) {
            return null;
        }
        return logNames.get(0);
    }

    /**
     * Determine whether the MySQL server has the binlog_row_image set to 'FULL'.
     *
     * @return {@code true} if the server's {@code binlog_row_image} is set to {@code FULL}, or {@code false} otherwise
     */
    protected boolean isBinlogRowImageFull(MySQLBinlogConfig mySQLBinlogConfig) {
        try {
            final String rowImage = queryAndMap(mySQLBinlogConfig,"SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'", rs -> {
                if (rs.next()) {
                    return rs.getString(2);
                }
                // This setting was introduced in MySQL 5.6+ with default of 'FULL'.
                // For older versions, assume 'FULL'.
                return "FULL";
            });
            logger.debug("binlog_row_image={}", rowImage);
            return "FULL".equalsIgnoreCase(rowImage);
        }
        catch (Exception e) {
            throw new DataImportException("Unexpected error while connecting to MySQL and looking at BINLOG_ROW_IMAGE mode: ", e);
        }
    }

    /**
     * Determine whether the MySQL server has the row-level binlog enabled.
     *
     * @return {@code true} if the server's {@code binlog_format} is set to {@code ROW}, or {@code false} otherwise
     */
    protected boolean isBinlogFormatRow(MySQLBinlogConfig mySQLBinlogConfig) {
        try {
            final String mode = queryAndMap(mySQLBinlogConfig, "SHOW GLOBAL VARIABLES LIKE 'binlog_format'", rs -> rs.next() ? rs.getString(2) : "");
            logger.debug("binlog_format={}", mode);
            return "ROW".equalsIgnoreCase(mode);
        } catch (Exception e) {
            throw new DataImportException("Unexpected error while connecting to MySQL and looking at BINLOG_FORMAT mode: ", e);
        }
    }

    /**
     * Determine the executed GTID set for MySQL.
     *
     * @return the string representation of MySQL's GTID sets; never null but an empty string if the server does not use GTIDs
     */
    public static String knownGtidSet(MySQLBinlogConfig mySQLBinlogConfig) {
        try {
            return queryAndMap(mySQLBinlogConfig,"SHOW MASTER STATUS", rs -> {
                if (rs.next() && rs.getMetaData().getColumnCount() > 4) {
                    return rs.getString(5); // GTID set, may be null, blank, or contain a GTID set
                }
                return "";
            });
        }
        catch (Exception e) {
            throw new DataImportException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }
    }

    /**
     * Determine whether the binlog position as set on the {@link MySQLBinlogListener} is available in the server.
     *
     * @return {@code true} if the server has the binlog coordinates, or {@code false} otherwise
     */
    public static boolean isBinlogAvailable(MySQLBinlogConfig mySQLBinlogConfig,MySQLBinlogListener mySQLBinlogListener) {
//        String gtidStr = mySQLBinlogListener.getLastGtid();
//        if (gtidStr != null) {
//            if (gtidStr.trim().isEmpty()) {
//                return true; // start at beginning ...
//            }
//            String availableGtidStr = knownGtidSet(mySQLBinlogConfig);
//            if (availableGtidStr == null || availableGtidStr.trim().isEmpty()) {
//                // Last offsets had GTIDs but the server does not use them ...
//                logger.info("Connector used GTIDs previously, but MySQL does not know of any GTIDs or they are not enabled");
//                return false;
//            }
//            // GTIDs are enabled, and we used them previously, but retain only those GTID ranges for the allowed source UUIDs ...
//            BBossGtidSet gtidSet = new BBossGtidSet(gtidStr).retainAll(mySQLBinlogConfig.gtidSourceFilter());
//            // Get the GTID set that is available in the server ...
//            BBossGtidSet availableGtidSet = new BBossGtidSet(availableGtidStr);
//            if (gtidSet.isContainedWithin(availableGtidSet)) {
//                logger.info("MySQL current GTID set {} does contain the GTID set required by the connector {}", availableGtidSet, gtidSet);
//                final BBossGtidSet knownServerSet = availableGtidSet.retainAll(mySQLBinlogConfig.gtidSourceFilter());
//                final BBossGtidSet gtidSetToReplicate = subtractGtidSet(knownServerSet, gtidSet);
//                final BBossGtidSet purgedGtidSet = purgedGtidSet();
//                logger.info("Server has already purged {} GTIDs", purgedGtidSet);
//                final BBossGtidSet nonPurgedGtidSetToReplicate = subtractGtidSet(gtidSetToReplicate, purgedGtidSet);
//                logger.info("GTIDs known by the server but not processed yet {}, for replication are available only {}", gtidSetToReplicate, nonPurgedGtidSetToReplicate);
//                if (!gtidSetToReplicate.equals(nonPurgedGtidSetToReplicate)) {
//                    logger.info("Some of the GTIDs needed to replicate have been already purged");
//                    return false;
//                }
//                return true;
//            }
//            logger.info("Connector last known GTIDs are {}, but MySQL has {}", gtidSet, availableGtidSet);
//            return false;
//        }
//

        // Accumulate the available binlog filenames ...
        List<BinFileInfo> logNames = availableBinlogFiles(mySQLBinlogConfig);

        return isBinlogAvailable(  mySQLBinlogConfig,  mySQLBinlogListener, logNames);

    }
    /**
     * Determine whether the binlog position as set on the {@link MySQLBinlogListener} is available in the server.
     *
     * @return {@code true} if the server has the binlog coordinates, or {@code false} otherwise
     */
    public static boolean isBinlogAvailable(MySQLBinlogConfig mySQLBinlogConfig,MySQLBinlogListener mySQLBinlogListener,List<BinFileInfo> logNames) {
//        String gtidStr = mySQLBinlogListener.getLastGtid();
//        if (gtidStr != null) {
//            if (gtidStr.trim().isEmpty()) {
//                return true; // start at beginning ...
//            }
//            String availableGtidStr = knownGtidSet(mySQLBinlogConfig);
//            if (availableGtidStr == null || availableGtidStr.trim().isEmpty()) {
//                // Last offsets had GTIDs but the server does not use them ...
//                logger.info("Connector used GTIDs previously, but MySQL does not know of any GTIDs or they are not enabled");
//                return false;
//            }
//            // GTIDs are enabled, and we used them previously, but retain only those GTID ranges for the allowed source UUIDs ...
//            BBossGtidSet gtidSet = new BBossGtidSet(gtidStr).retainAll(mySQLBinlogConfig.gtidSourceFilter());
//            // Get the GTID set that is available in the server ...
//            BBossGtidSet availableGtidSet = new BBossGtidSet(availableGtidStr);
//            if (gtidSet.isContainedWithin(availableGtidSet)) {
//                logger.info("MySQL current GTID set {} does contain the GTID set required by the connector {}", availableGtidSet, gtidSet);
//                final BBossGtidSet knownServerSet = availableGtidSet.retainAll(mySQLBinlogConfig.gtidSourceFilter());
//                final BBossGtidSet gtidSetToReplicate = subtractGtidSet(knownServerSet, gtidSet);
//                final BBossGtidSet purgedGtidSet = purgedGtidSet();
//                logger.info("Server has already purged {} GTIDs", purgedGtidSet);
//                final BBossGtidSet nonPurgedGtidSetToReplicate = subtractGtidSet(gtidSetToReplicate, purgedGtidSet);
//                logger.info("GTIDs known by the server but not processed yet {}, for replication are available only {}", gtidSetToReplicate, nonPurgedGtidSetToReplicate);
//                if (!gtidSetToReplicate.equals(nonPurgedGtidSetToReplicate)) {
//                    logger.info("Some of the GTIDs needed to replicate have been already purged");
//                    return false;
//                }
//                return true;
//            }
//            logger.info("Connector last known GTIDs are {}, but MySQL has {}", gtidSet, availableGtidSet);
//            return false;
//        }
//
        String binlogFilename = mySQLBinlogListener.getBinlogFile();
        Long position = mySQLBinlogListener.getPosition();
        if (binlogFilename == null) {
            return true; // start at current position
        }
        if (binlogFilename.equals("")) {
            return true; // start at beginning
        }

        if(logNames == null || logNames.size() == 0){
            return true;
        }
//        // Accumulate the available binlog filenames ...
//        List<String> logNames = availableBinlogFiles(mySQLBinlogConfig);

        // And compare with the one we're supposed to use ...
        boolean found = false;
        BinFileInfo binFileInfo = null;
        StringBuilder names = new StringBuilder();
        for(BinFileInfo _binFileInfo:logNames){
            if(names.length() > 0)
                names.append(",");
            names.append(_binFileInfo.getFileName());

            if(_binFileInfo.getFileName().equals(binlogFilename)){
                binFileInfo = _binFileInfo;
                break;
            }
        }
        if(binFileInfo != null){
            found = true;
            if(position != null && binFileInfo.getFileSize() < position){
                mySQLBinlogListener.setPosition(null);
                mySQLBinlogListener.setBinlogFile(null);
                mySQLBinlogListener.setLastGtid(null);
            }
        }


        if (!found) {
            if (logger.isInfoEnabled()) {
                logger.info("Connector requires binlog file '{}', but MySQL only has {}", binlogFilename, names.toString());
            }
        }
        else {
            logger.info("MySQL has the binlog file '{}' required by the connector", binlogFilename);
        }
//
        return found;

    }

    /**
     * Query the database server to get the list of the binlog files availble.
     *
     * @return list of the binlog files
     */
    public static List<BinFileInfo> availableBinlogFiles(MySQLBinlogConfig mySQLBinlogConfig) {
        List<BinFileInfo> logNames = new ArrayList<>();
        try {
            logger.info("Get all known binlogs from MySQL");
            queryAndMap(mySQLBinlogConfig,"SHOW BINARY LOGS", rs -> {
                while (rs.next()) {
                    BinFileInfo binFileInfo = new BinFileInfo();
                    binFileInfo.setFileName(rs.getString(1));
                    binFileInfo.setFileSize(rs.getLong(2));
                    logNames.add(binFileInfo);
                }
                return logNames;
            });
            return logNames;
        }
        catch (Exception e) {
            throw new DataImportException("Unexpected error while connecting to MySQL and looking for binary logs: ", e);
        }
    }
}
