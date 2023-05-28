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
    private String host;
    private int port;
    private String dbUser;
    private String dbPassword;


    private String database;
    private String tables;
    private String[] listenTables;
    private String[] collectorFileNames;

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

    private Boolean keepAlive ;
    private Long keepAliveInterval ;

    private Long heartbeatInterval;
    private Boolean blocking;

    private Long connectTimeout ;

    private Boolean gtidSetFallbackToPurged;
    private Boolean useBinlogFilenamePositionInGtidMode;

    private SSLMode sslMode;

    private SSLSocketFactory sslSocketFactory;

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

    public String[] getCollectorFileNames() {
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

    @Override
    public void build(ImportBuilder importBuilder) {
        if(SimpleStringUtil.isEmpty(database))
            throw new DataImportException("listern database can't be empty.");
        if(SimpleStringUtil.isEmpty(tables))
            throw new DataImportException("listern tables can't be empty.");
        listenTables = tables.split(",");
        if(SimpleStringUtil.isEmpty(schemaUrl)){
            schemaUrl = "jdbc:mysql://" + getHost()+":" +getPort()+
                       "/INFORMATION_SCHEMA?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
        }
        if(SimpleStringUtil.isNotEmpty(fileNames) )
            collectorFileNames = fileNames.split(",");
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

    public String[] getListenTables() {
        return listenTables;
    }

    public MySQLBinlogConfig setListenTables(String[] listenTables) {
        this.listenTables = listenTables;
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
}
