package org.frameworkset.tran.plugin.db.input;
/**
 * Copyright 2022 bboss
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
import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.db.BaseDBConfig;
import org.frameworkset.tran.record.RecordBuidler;

import javax.sql.DataSource;
import java.sql.ResultSet;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/19
 * @author biaoping.yin
 * @version 1.0
 */
public class DBInputConfig extends BaseDBConfig<DBInputConfig> implements InputConfig<DBInputConfig> {

	private String sourceDbname;
	protected String sql;
	private String sqlFilepath;
	protected String sqlName;

	private Boolean enableDBTransaction;
    private Integer fetchSize;
    private boolean enableLocalDate;

    private Boolean parallelDatarefactor;

    private RecordBuidler<ResultSet> recordBuidler;
    
    public DBInputConfig setEnableLocalDate(boolean enableLocalDate){
        this.enableLocalDate = enableLocalDate;
        return this;
    }
    public boolean isParallelDatarefactor(){
        if(parallelDatarefactor != null)
            return parallelDatarefactor;
        return false;
    }

    /**
     * 并行Datarefactor处理需要设置RecordBuidler，默认为DBRecordBuilder，如果需要自定义resultset record，从DBRecordBuilder继承实现方法即可：
     * {@code public Map<String, Object> build(RecordBuidlerContext<ResultSet> recordBuidlerContext) throws DataImportException}
     * @param parallelDatarefactor
     * @return
     */
    public DBInputConfig setParallelDatarefactor(boolean parallelDatarefactor) {
        this.parallelDatarefactor = parallelDatarefactor;
        return this;
    }
    
    

    @Override
    public boolean enableLocalDate() {
        return enableLocalDate;
    }

    public String getSourceDbname() {
		return sourceDbname;
	}
//
//	public DBInputConfig setSourceDbname(String sourceDbname) {
//		this.sourceDbname = sourceDbname;
//		return this;
//	}
	public String getDBName(){
		DBConfig dbConfig = getDbConfig();
		if(dbConfig != null && SimpleStringUtil.isNotEmpty(dbConfig.getDbName()) ){
			return dbConfig.getDbName();
		}
		if(sourceDbname != null){
			return sourceDbname;
		}

		return null;
	}
	public DBConfig getDbConfig() {
		if(dbConfig == null ){
			if(sourceDbname != null){
				return dbConfigMap.get(sourceDbname);
			}
		}
		return dbConfig;
	}
	public DBInputConfig setDbConfig(DBConfig dbConfig) {
		this.dbConfig = dbConfig;
		if(dbConfig != null && SimpleStringUtil.isNotEmpty(dbConfig.getDbName()) )
			this.dbConfigMap.put(dbConfig.getDbName(),dbConfig);

		return this;
	}

	public DBInputConfig setSql(String sql) {
		this.sql = sql;
		return this;
	}

	public String getSql() {
		return sql;
	}

	public DBInputConfig setSqlFilepath(String sqlFilepath) {
		this.sqlFilepath = sqlFilepath;
		return this;
	}

	public String getSqlFilepath() {
		return sqlFilepath;
	}

	public DBInputConfig setSqlName(String sqlName) {
		this.sqlName = sqlName;
		return this;
	}

	public String getSqlName() {
		return sqlName;
	}

	@Override
	public void build(ImportContext importContext,ImportBuilder importBuilder) {

		if(SimpleStringUtil.isEmpty(this.getSql())){
			if(SimpleStringUtil.isEmpty(getSqlFilepath()) || SimpleStringUtil.isEmpty(getSqlName()) ){
				throw new DataImportException("Input sql is not setted.");
			}
		}
		if(dbConfig == null ){

			dbConfig = importBuilder.getDefaultDBConfig();
			this.dbConfigMap.put(dbConfig.getDbName(),dbConfig);
		}
		if(dbConfig == null){
			throw new DataImportException("Source DB Config not config to dbinputconfig.");
		}
		if(SimpleStringUtil.isEmpty(sourceDbname))
			sourceDbname = dbConfig.getDbName();

        if(importBuilder.isSetFetchSized() && fetchSize == null) {
//            if (dbConfig.getJdbcFetchSize() == null) {
//                setJdbcFetchSize(importBuilder.getFetchSize());
//            }
            fetchSize = importBuilder.getFetchSize();
        }
        if(fetchSize != null){
            super.setJdbcFetchSize(fetchSize);
        }
        if(isParallelDatarefactor()){
            if(recordBuidler == null){
                recordBuidler = new DBRecordBuilder();
            }
        }
        if(dbConfig != null){
            dbConfig.setDataSource(dataSource);
        }

	}

    public Integer getFetchSize() {
        return fetchSize;
    }

    @Override
	public InputPlugin getInputPlugin(ImportContext importContext) {
		return new DBInputDataTranPlugin(   importContext );
	}
 
 

    /**
     * 插件查询jdbcFetchSize设置，每次执行查询请求时进行设置
     * @param jdbcFetchSize
     * @return
     */
	public DBInputConfig setJdbcFetchSize(Integer jdbcFetchSize) {
//		_setJdbcFetchSize(  jdbcFetchSize);
        this.fetchSize = jdbcFetchSize;
		return  this;
	}

 
	public DBConfig getDBConfig(String dbname){
		return dbConfigMap.get(dbname);
	}
	public Boolean getEnableDBTransaction() {
		return enableDBTransaction;
	}
	public DBInputConfig setDbName(String dbName) {
		super.setDbName(  dbName);
		this.sourceDbname = dbName;

		return this;
	}
  

	public DBInputConfig setEnableDBTransaction(Boolean enableDBTransaction) {
		this.enableDBTransaction = enableDBTransaction;
		return this;
	}

   
    public RecordBuidler<ResultSet> getRecordBuidler() {
        return recordBuidler;
    }
    /**
     * 并行Datarefactor处理标记为parallelDatarefactor=true时，需要设置RecordBuidler，默认为DBRecordBuilder，如果需要自定义resultset record，从DBRecordBuilder继承实现方法即可：
     * {@code public Map<String, Object> build(RecordBuidlerContext<ResultSet> recordBuidlerContext) throws DataImportException}
     * @param recordBuidler
     * @return
     */
    public DBInputConfig setRecordBuidler(RecordBuidler recordBuidler) {
        this.recordBuidler = (RecordBuidler<ResultSet>)recordBuidler;
        return this;
    }

    /**
     * 设置外部数据源
     * @param dataSource
     * @return
     */
    public DBInputConfig setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }
}
