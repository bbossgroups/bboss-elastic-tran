package org.frameworkset.tran.plugin.db.input;
/**
 * Copyright 2008 biaoping.yin
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

import com.frameworkset.common.poolman.ConfigSQLExecutor;
import com.frameworkset.common.poolman.SQLExecutor;
import com.frameworkset.common.poolman.handle.ResultSetHandler;
import com.frameworkset.common.poolman.util.DBOptions;
import com.frameworkset.orm.transaction.TransactionManager;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPluginImpl;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.db.BaseDBPlugin;
import org.frameworkset.tran.schedule.SQLInfo;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.CommonAsynRecordTranJob;
import org.frameworkset.tran.task.CommonRecordTranJob;
import org.frameworkset.tran.task.TaskFailedException;
import org.frameworkset.tran.task.TranJob;
import org.frameworkset.util.tokenizer.TextGrammarParser;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class DBInputDataTranPlugin extends BaseDBPlugin implements InputPlugin {
	protected String jobType ;
	public DBInputDataTranPlugin(ImportContext importContext) {
		super(importContext);
		dbInputConfig = (DBInputConfig) importContext.getInputConfig();
		this.jobType = "DBInputDataTranPlugin";
	}


	protected SQLInfo sqlInfo;
	protected ConfigSQLExecutor executor;
	protected DBInputConfig dbInputConfig;
	@Override
	public void beforeInit(){
//		super.init(importContext,targetImportContext);

	}
	@Override
	public void init() {
//		this.initES(importContext.getApplicationPropertiesFile());
		DataTranPluginImpl.initDS(dbStartResult,dbInputConfig.getDbConfig());
//		initOtherDSes(importContext.getConfigs());
		this.initSourceSQLInfo();

	}
    @Override
    public TranJob getTranJob(){
        if(dbInputConfig.isParallelDatarefactor())
            return new CommonAsynRecordTranJob();
        else 
            return new CommonRecordTranJob();
    }
	@Override
	public String getJobType() {
		return jobType;
	}


	@Override
	public void afterInit(){
		if(sqlInfo != null
				&& sqlInfo.getParamSize() > 0
				&& !dataTranPlugin.isIncreamentImport() && this.importContext.getJobInputParams() == null){
			throw new TaskFailedException("1.Parameter variables cannot be set in non-increament import SQL statements："+dbInputConfig.getSql() +"\r\n2.Parameter values must be setted by BaseImportBuilder.addParam(String,Object) method.");
		}
//		this.externalTimer = this.importContext.isExternalTimer();
	}
	@Override
	public void initStatusTableId() {
		if(dataTranPlugin.isIncreamentImport()) {
			if(dbInputConfig.getSql() != null && !dbInputConfig.getSql().equals("")) {
				//计算增量记录id
				importContext.setStatusTableId(dbInputConfig.getSql().hashCode());
			}
			else{
				String sqlFile = dbInputConfig.getSqlFilepath();
				String sqlName = dbInputConfig.getSqlName();
				//计算增量记录id
				importContext.setStatusTableId((sqlFile+"$$"+sqlName ).hashCode());
			}
		}

	}
	public void initSourceSQLInfo(){

		if(dbInputConfig.getSql() == null || dbInputConfig.getSql().equals("")){

			if(dbInputConfig.getSqlFilepath() != null && !dbInputConfig.getSqlFilepath().equals(""))
				try {
					ConfigSQLExecutor executor = new ConfigSQLExecutor(dbInputConfig.getSqlFilepath());
					org.frameworkset.persitent.util.SQLInfo sqlInfo = executor.getSqlInfo(getSourceDBName(),dbInputConfig.getSqlName());
					this.executor = executor;
					dbInputConfig.setSql(sqlInfo.getSql());
				}
				catch (SQLException e){
					throw ImportExceptionUtil.buildDataImportException(importContext,e);
				}

		}
		if(dbInputConfig.getSql() != null && !dbInputConfig.getSql().equals("")) {

			initSQLInfoParams();
		}

	}
	private void initSQLInfoParams(){
		String originSQL = dbInputConfig.getSql();
		List<TextGrammarParser.GrammarToken> tokens =
				TextGrammarParser.parser(originSQL, "#[", "]");
		SQLInfo _sqlInfo = new SQLInfo();
		int paramSize = 0;
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < tokens.size(); i ++){
			TextGrammarParser.GrammarToken token = tokens.get(i);
			if(token.texttoken()){
				builder.append(token.getText());
			}
			else {
				builder.append("?");
				if(paramSize == 0 && !importContext.isLastValueColumnSetted() && importContext.isImportIncreamentConfigSetted()){//如果没有指定增量列名称，则默认使用第一个变量参数作为增量字段
					_sqlInfo.setLastValueVarName(token.getText());
				}
				paramSize ++;

			}
		}
		_sqlInfo.setParamSize(paramSize);
		_sqlInfo.setSql(builder.toString());
		this.sqlInfo = _sqlInfo;


	}
	public SQLInfo getSqlInfo() {
		return sqlInfo;
	}

	@Override
	public String getLastValueVarName(){
		if(!importContext.isLastValueColumnSetted() && importContext.isImportIncreamentConfigSetted()) {
			return this.sqlInfo != null ? this.sqlInfo.getLastValueVarName() : null;
		}
		else{
			return super.getLastValueVarName();
		}
	}




	private void commonImportData(TaskContext taskContext,ResultSetHandler resultSetHandler) throws Exception {
		String sourceDBName = getSourceDBName();
		boolean isEnableDBTransaction = dbInputConfig.getDbConfig() != null? dbInputConfig.getDbConfig().isEnableDBTransaction():false;
		DBOptions dbOptions = getDBOptions();
		Map params = dataTranPlugin.getJobInputParams(taskContext);
		if(params == null || params.size() == 0 ) {
			if (importContext.getDataRefactor() == null || !isEnableDBTransaction) {
				if (executor == null) {
					SQLExecutor.queryWithDBNameByNullRowHandler(dbOptions, resultSetHandler, sourceDBName, dbInputConfig.getSql());
				} else {
					executor.queryWithDBNameByNullRowHandler(dbOptions, resultSetHandler, sourceDBName, dbInputConfig.getSqlName());
				}
			} else {

				TransactionManager transactionManager = new TransactionManager();
				try {
					transactionManager.begin(TransactionManager.RW_TRANSACTION);
					if (executor == null) {
						SQLExecutor.queryWithDBNameByNullRowHandler(dbOptions, resultSetHandler, sourceDBName, dbInputConfig.getSql());
					} else {
						executor.queryWithDBNameByNullRowHandler(dbOptions, resultSetHandler, sourceDBName, dbInputConfig.getSqlName());
					}
					transactionManager.commit();
				} finally {
					transactionManager.releasenolog();
				}
			}
		}
		else{
			if (importContext.getDataRefactor() == null || !isEnableDBTransaction) {
				if (executor == null) {
					SQLExecutor.queryBeanWithDBNameByNullRowHandler(dbOptions, resultSetHandler, sourceDBName, dbInputConfig.getSql(),params);
				} else {
					executor.queryBeanWithDBNameByNullRowHandler(dbOptions, resultSetHandler, sourceDBName, dbInputConfig.getSqlName(),params);
				}
			} else {

				TransactionManager transactionManager = new TransactionManager();
				try {
					transactionManager.begin(TransactionManager.RW_TRANSACTION);
					if (executor == null) {
						SQLExecutor.queryBeanWithDBNameByNullRowHandler(dbOptions, resultSetHandler, sourceDBName, dbInputConfig.getSql(),params);
					} else {
						executor.queryBeanWithDBNameByNullRowHandler(dbOptions, resultSetHandler, sourceDBName, dbInputConfig.getSqlName(),params);
					}
					transactionManager.commit();
				} finally {
					transactionManager.releasenolog();
				}
			}
		}
	}

	private String getSourceDBName(){
		DBConfig dbConfig = dbInputConfig.getDbConfig();
		String sourceDBName  = dbInputConfig.getSourceDbname();
		if(sourceDBName == null){
			if(dbConfig != null && SimpleStringUtil.isNotEmpty(dbConfig.getDbName()))
				sourceDBName = dbConfig.getDbName();
		}
		if(sourceDBName == null){

			throw ImportExceptionUtil.buildDataImportException(importContext,"Please set dbname use dbInputConfig.setDbName(dbname).");
		}
		return sourceDBName;

	}
	private DBOptions getDBOptions(){
		DBOptions dbOptions = null;
        Integer fetchSize = dbInputConfig.getFetchSize();
		if(fetchSize != null && fetchSize != 0) {
            dbOptions = new DBOptions();
            dbOptions.setFetchSize(fetchSize);
		}
		return dbOptions;
	}
	private void increamentImportData(TaskContext taskContext,ResultSetHandler resultSetHandler) throws Exception {
		String sourceDBName = getSourceDBName();
		DBOptions dbOptions = getDBOptions();
		Map params = dataTranPlugin.getJobInputParams(  taskContext);
		boolean isEnableDBTransaction = dbInputConfig.getDbConfig() != null? dbInputConfig.getDbConfig().isEnableDBTransaction():false;
		if(importContext.getDataRefactor() == null || !isEnableDBTransaction){
			if (executor == null) {
				SQLExecutor.queryBeanWithDBNameByNullRowHandler(dbOptions,resultSetHandler, sourceDBName, dbInputConfig.getSql(), dataTranPlugin.getParamValue(params));
			} else {
				executor.queryBeanWithDBNameByNullRowHandler(dbOptions,resultSetHandler, sourceDBName, dbInputConfig.getSqlName(), dataTranPlugin.getParamValue(params));

			}
		}
		else {
			TransactionManager transactionManager = new TransactionManager();
			try {
				transactionManager.begin(TransactionManager.RW_TRANSACTION);
				if (executor == null) {
					SQLExecutor.queryBeanWithDBNameByNullRowHandler(dbOptions,resultSetHandler, sourceDBName, dbInputConfig.getSql(), dataTranPlugin.getParamValue(params));
				} else {
					executor.queryBeanWithDBNameByNullRowHandler(dbOptions,resultSetHandler, sourceDBName, dbInputConfig.getSqlName(), dataTranPlugin.getParamValue(params));

				}
			} finally {
				transactionManager.releasenolog();
			}
		}
	}
	@Override
	public void doImportData( TaskContext taskContext)  throws DataImportException {
		ResultSetHandler resultSetHandler = new DefaultResultSetHandler( taskContext, importContext,dataTranPlugin);

		try {
			if (sqlInfo.getParamSize() == 0) {

				commonImportData(taskContext,resultSetHandler);

			} else {
				if (!dataTranPlugin.isIncreamentImport()) {
//					setForceStop();
					commonImportData(taskContext,resultSetHandler);
				} else {

					increamentImportData(taskContext,resultSetHandler);

				}
			}
		}
		catch (DataImportException e){
			throw e;
		}
		catch (Exception e){
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}
	}










}
