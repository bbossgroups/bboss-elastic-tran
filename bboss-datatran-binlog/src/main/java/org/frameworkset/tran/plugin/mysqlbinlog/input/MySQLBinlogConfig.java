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
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.db.CDCDBTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.function.Predicate;

import static org.frameworkset.tran.metrics.job.MetricsConfig.DEFAULT_metricsInterval;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/4/5
 * @author biaoping.yin
 * @version 1.0
 */
public class MySQLBinlogConfig extends BaseConfig implements InputConfig {
    private static Logger logger = LoggerFactory.getLogger(MySQLBinlogConfig.class);
    private String host;
    private int port;
    private String dbUser;
    private String dbPassword;

    public static final String split = "$$";

    private String database;
    private String tables;
    private Map<String,List<CDCDBTable>> dbTables;
    private Map<String,Map<String,CDCDBTable>> dbTableIdxs;
    private List<BinFileInfo> collectorFileNames;

    /**
     * String url =  "jdbc:mysql://" + host.getHost()+":" +host.getPort()+
     *                     "/INFORMATION_SCHEMA?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
     */
    private String schemaUrl;


    /**
     * 默认十分钟执行一次拦截器监控数据afterCall方法
     */
    private long metricsInterval = DEFAULT_metricsInterval;
    /**
     * mysql servierId
     * 可以通过以下命令查看：show variables like 'server_id';
     */
    //
    private Long  serverId;
    /**
     * 需要采集的binlogs文件清单
     */
    private String fileNames;

    private Long position;


    private long joinToConnectTimeOut = 0L;

    private String mastterBinLogFile;



    private boolean collectMasterHistoryBinlog;



    private String binlogDir;

    private String gtId;



    private boolean enableIncrement;

    private Boolean keepAlive ;
    private Long keepAliveInterval ;

    private Long heartbeatInterval;
    private Boolean blocking;

    private Long connectTimeout ;

    private Boolean gtidSetFallbackToPurged;
    private Boolean useBinlogFilenamePositionInGtidMode;


    private boolean gtidEnabled;
    private SSLMode sslMode;

    private SSLSocketFactory sslSocketFactory;
    private boolean ddlSyn;
    private String ddlSynDatabases;
    private String[] listDdlSynDatabases;


    /**
     * 增量采集
     */
    private boolean increamentFrom001;

    public String getHost() {
        return host;
    }

    public MySQLBinlogConfig setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public MySQLBinlogConfig setPort(int port) {
        this.port = port;
        return this;
    }

    public String getDbUser() {
        return dbUser;
    }

    public MySQLBinlogConfig setDbUser(String dbUser) {
        this.dbUser = dbUser;
        return this;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public MySQLBinlogConfig setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
        return this;
    }

    public Long getServerId() {
        return serverId;
    }

    public MySQLBinlogConfig setServerId(Long serverId) {
        this.serverId = serverId;
        return this;
    }

    public String getFileNames() {
        return fileNames;
    }

    public List<BinFileInfo> getCollectorFileNames() {
        return collectorFileNames;
    }

    public MySQLBinlogConfig setFileNames(String fileNames) {
        this.fileNames = fileNames;
        return this;
    }

    public long getMetricsInterval() {
        return metricsInterval;
    }

    public MySQLBinlogConfig setMetricsInterval(long metricsInterval) {
        this.metricsInterval = metricsInterval;
        return this;
    }

    private void addDBTable(String db,String table){
        Map<String, CDCDBTable> dbCDCDBTableMap = this.dbTableIdxs.get(db);
        List<CDCDBTable> cdcdbTables = this.dbTables.get(db);
        if(dbCDCDBTableMap == null){
            dbCDCDBTableMap = new LinkedHashMap<>();
            cdcdbTables = new ArrayList<>();
            this.dbTableIdxs.put(db,dbCDCDBTableMap);
            dbTables.put(db,cdcdbTables);
        }
        CDCDBTable cdcdbTable = new CDCDBTable();
        cdcdbTable.setTableName(table);
        cdcdbTable.setDatabase(db);
        dbCDCDBTableMap.put(table,cdcdbTable);
        cdcdbTables.add(cdcdbTable);
    }
    private void addCommonDBTable(List<String> tables){
        Iterator<Map.Entry<String, Map<String, CDCDBTable>>> iterator = dbTableIdxs.entrySet().iterator();
        for(String table:tables) {
            while (iterator.hasNext()) {
                Map.Entry<String, Map<String, CDCDBTable>> entry = iterator.next();
                Map<String, CDCDBTable> tableMap = entry.getValue();
                if(!tableMap.containsKey(table)){
                    CDCDBTable cdcdbTable = new CDCDBTable();
                    cdcdbTable.setDatabase(entry.getKey());
                    cdcdbTable.setTableName(table);
                    tableMap.put(table,cdcdbTable);
                    dbTables.get(entry.getKey()).add(cdcdbTable);
                }
            }
        }

    }

    public Map<String, List<CDCDBTable>> getDbTables() {
        return dbTables;
    }

    public Map<String, Map<String, CDCDBTable>> getDbTableIdxs() {
        return dbTableIdxs;
    }

    private void parserMeta(){
        this.dbTableIdxs = new LinkedHashMap<>();
        this.dbTables = new LinkedHashMap<>();
        if(SimpleStringUtil.isNotEmpty(database)){
            String[] _dbs = database.split(",");

            for(String db:_dbs){
                dbTableIdxs.put(db,new LinkedHashMap<>());
                dbTables.put(db,new ArrayList<>());
            }
        }
        List<String> commonTables = new ArrayList<>();
        String[] _listenTables = tables.split(",");
        for(String table:_listenTables){
            if(table.indexOf(".") > 0){
                String t[] = table.split("\\.");
                addDBTable(t[0],t[1]);
            }
            else{
                commonTables.add(table);
            }
        }
        if(SimpleStringUtil.isNotEmpty(commonTables )) {
            if(SimpleStringUtil.isEmpty(database)) {
                throw new DataImportException("Listern database can't be empty for tables"+SimpleStringUtil.object2json(commonTables)+".");
            }
            addCommonDBTable(commonTables);

        }


    }

    public String[] getListDdlSynDatabases() {
        return listDdlSynDatabases;
    }

    public boolean isDdlSynDatabases(String database){
        for(String db:listDdlSynDatabases){
            if(db.equals(database)){
                return true;
            }
        }
        return false;
    }


    @Override
    public void build(ImportBuilder importBuilder) {

        if(isDdlSyn()){
            if(SimpleStringUtil.isEmpty(ddlSynDatabases))
                throw new DataImportException("DdlSynDatabases can't be empty.Use MySQLBinlogConfig.setDdlSynDatabases method to set DdlSynDatabases!");
            listDdlSynDatabases = ddlSynDatabases.split(",");
        }
        if(SimpleStringUtil.isEmpty(tables))
            throw new DataImportException("listern tables can't be empty.");
        parserMeta();

        if(SimpleStringUtil.isEmpty(schemaUrl)){
            schemaUrl = "jdbc:mysql://" + getHost()+":" +getPort()+
                       "/INFORMATION_SCHEMA?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
        }
        if(SimpleStringUtil.isNotEmpty(fileNames) ) {
            String[] _collectorFileNames = fileNames.split(",");
            collectorFileNames = new ArrayList<>();
            for(String name:_collectorFileNames){
                File file = new File(name);
                if(file.exists()) {
                    BinFileInfo binFileInfo = new BinFileInfo();
                    binFileInfo.setFileName(name);
                    collectorFileNames.add(binFileInfo);
                }
                else{
                    logger.warn("{} do not exist.",name);
                }
            }
        }
        if(isEnableIncrement() ){
//            this.enableIncrement = false;//禁用不支持的增量模式
            if(position == null)
                position = 0L;
        }
        if(isCollectMasterHistoryBinlog()){
            if(SimpleStringUtil.isEmpty(this.getBinlogDir())){
                throw new DataImportException("isCollectMasterHistoryBinlog but binlog dir is not setted. Use MySQLBinlogConfig.setBinlogDir() to set binlog dir.");
            }
        }
    }

    @Override
    public InputPlugin getInputPlugin(ImportContext importContext) {
        return new MysqlBinlogInputDatatranPlugin(importContext);
    }

    @Override
    public DataTranPlugin buildDataTranPlugin(ImportContext importContext){
        DataTranPlugin dataTranPlugin = new MysqlBinlogDataTranPluginImpl(importContext);
        return dataTranPlugin;
    }
    public String getDatabase() {
        return database;
    }

    public MySQLBinlogConfig setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getTables() {
        return tables;
    }

    public MySQLBinlogConfig setTables(String tables) {
        this.tables = tables;
        return this;
    }



    public Long getPosition() {
        return position;
    }

    public MySQLBinlogConfig setPosition(Long position) {
        this.position = position;
        return this;
    }
    public String getSchemaUrl() {
        return schemaUrl;
    }

    public MySQLBinlogConfig setSchemaUrl(String schemaUrl) {
        this.schemaUrl = schemaUrl;
        return this;
    }


    public Boolean getKeepAlive() {
        return keepAlive;
    }

    public MySQLBinlogConfig setKeepAlive(Boolean keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    public Long getKeepAliveInterval() {
        return keepAliveInterval;
    }

    public MySQLBinlogConfig setKeepAliveInterval(Long keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
        return this;
    }

    public Long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public MySQLBinlogConfig setHeartbeatInterval(Long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    public Boolean getBlocking() {
        return blocking;
    }

    public MySQLBinlogConfig setBlocking(Boolean blocking) {
        this.blocking = blocking;
        return this;
    }

    public Long getConnectTimeout() {
        return connectTimeout;
    }

    public MySQLBinlogConfig setConnectTimeout(Long connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public Boolean getGtidSetFallbackToPurged() {
        return gtidSetFallbackToPurged;
    }

    public MySQLBinlogConfig setGtidSetFallbackToPurged(Boolean gtidSetFallbackToPurged) {
        this.gtidSetFallbackToPurged = gtidSetFallbackToPurged;
        return this;
    }

    public Boolean getUseBinlogFilenamePositionInGtidMode() {
        return useBinlogFilenamePositionInGtidMode;
    }

    public MySQLBinlogConfig setUseBinlogFilenamePositionInGtidMode(Boolean useBinlogFilenamePositionInGtidMode) {
        this.useBinlogFilenamePositionInGtidMode = useBinlogFilenamePositionInGtidMode;
        return this;
    }

    public SSLSocketFactory getSslSocketFactory() {
        return sslSocketFactory;
    }

    public MySQLBinlogConfig setSslSocketFactory(SSLSocketFactory sslSocketFactory) {
        this.sslSocketFactory = sslSocketFactory;
        return this;
    }

    public SSLMode getSslMode() {
        return sslMode;
    }

    public MySQLBinlogConfig setSslMode(SSLMode sslMode) {
        this.sslMode = sslMode;
        return this;
    }
    public boolean isEnableIncrement() {
        return enableIncrement;
    }

    /**
     * 本参数不起作用
     * @param enableIncrement
     * @return
     */
    public MySQLBinlogConfig setEnableIncrement(boolean enableIncrement) {
        this.enableIncrement = enableIncrement;
        return this;
    }
    @Override
    public boolean isSortedDefault(){
        return true;
    }

    public String getMastterBinLogFile() {
        return mastterBinLogFile;
    }

    public MySQLBinlogConfig setMastterBinLogFile(String mastterBinLogFile) {
        this.mastterBinLogFile = mastterBinLogFile;
        return this;
    }

    public long getJoinToConnectTimeOut() {
        return joinToConnectTimeOut;
    }

    /**
     * 设置异步启动client等待时间
     * @param joinToConnectTimeOut
     * @return
     */
    public MySQLBinlogConfig setJoinToConnectTimeOut(long joinToConnectTimeOut) {
        this.joinToConnectTimeOut = joinToConnectTimeOut;
        return this;
    }
    public String[] getColumns(String database, String table) {
        Map<String, CDCDBTable> tableMap = this.dbTableIdxs.get(database);
        if(tableMap != null){
            CDCDBTable cdcdbTable = tableMap.get(table);
            if(cdcdbTable != null){
                return cdcdbTable.getTableColumns();
            }
        }
        return null;
//        return this.allTableColumns.get(DBMetaUtil.buildTableKey(database,table));
    }

    public boolean isDdlSyn() {
        return ddlSyn;
    }

    public MySQLBinlogConfig setDdlSyn(boolean ddlSyn) {
        this.ddlSyn = ddlSyn;
        return this;
    }

    public String getDdlSynDatabases() {
        return ddlSynDatabases;
    }

    public MySQLBinlogConfig setDdlSynDatabases(String ddlSynDatabases) {
        this.ddlSynDatabases = ddlSynDatabases;
        return this;
    }
    public String getGtId() {
        return gtId;
    }

    public MySQLBinlogConfig setGtId(String gtId) {
        this.gtId = gtId;
        return this;
    }

    public Predicate<String> gtidSourceFilter() {
        return null;
    }
    public boolean isGtidEnabled() {
        return gtidEnabled;
    }

    public MySQLBinlogConfig setGtidEnabled(boolean gtidEnabled) {
        this.gtidEnabled = gtidEnabled;
        return this;
    }
    public boolean isIncreamentFrom001() {
        return increamentFrom001;
    }

    public MySQLBinlogConfig setIncreamentFrom001(boolean increamentFrom001) {
        this.increamentFrom001 = increamentFrom001;
        return this;
    }

    public boolean isCollectMasterHistoryBinlog() {
        return collectMasterHistoryBinlog;
    }

    public MySQLBinlogConfig setCollectMasterHistoryBinlog(boolean collectMasterHistoryBinlog) {
        this.collectMasterHistoryBinlog = collectMasterHistoryBinlog;
        return this;
    }
    public String getBinlogDir() {
        return binlogDir;
    }

    public MySQLBinlogConfig setBinlogDir(String binlogDir) {
        this.binlogDir = binlogDir;
        return this;
    }
}
