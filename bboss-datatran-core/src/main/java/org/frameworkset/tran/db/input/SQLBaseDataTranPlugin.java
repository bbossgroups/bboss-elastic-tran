package org.frameworkset.tran.db.input;
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
import com.frameworkset.orm.transaction.TransactionManager;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.DBContext;
import org.frameworkset.tran.schedule.SQLInfo;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskFailedException;
import org.frameworkset.util.tokenizer.TextGrammarParser;

import java.sql.SQLException;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/31 22:37
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class SQLBaseDataTranPlugin extends BaseDataTranPlugin {
	public SQLBaseDataTranPlugin(ImportContext importContext,ImportContext targetImportContext) {
		super(importContext,  targetImportContext);
	}
	protected SQLInfo sqlInfo;
	protected ConfigSQLExecutor executor;
	protected DBContext dbContext;
	@Override
	public void init(ImportContext importContext,ImportContext targetImportContext){
		super.init(importContext,targetImportContext);
		dbContext = (DBContext)importContext;
	}
	@Override
	public void beforeInit() {
//		this.initES(importContext.getApplicationPropertiesFile());
		this.initDS(importContext.getDbConfig());
		initOtherDSes(importContext.getConfigs());
//		initOtherDSes(importContext.getConfigs());
		this.initSourceSQLInfo();

	}

	@Override
	public void afterInit(){
		if(sqlInfo != null
				&& sqlInfo.getParamSize() > 0
				&& !this.isIncreamentImport()){
			throw new TaskFailedException("Parameter variables cannot be set in non-incremental import SQL statements："+dbContext.getSql());
		}
//		this.externalTimer = this.importContext.isExternalTimer();
	}
	@Override
	public void initStatusTableId() {
		if(isIncreamentImport()) {
			if(dbContext.getSql() != null && !dbContext.getSql().equals("")) {
				//计算增量记录id
				importContext.setStatusTableId(dbContext.getSql().hashCode());
			}
		}

	}
	public void initSourceSQLInfo(){

		if(dbContext.getSql() == null || dbContext.getSql().equals("")){

			if(dbContext.getSqlFilepath() != null && !dbContext.getSqlFilepath().equals(""))
				try {
					ConfigSQLExecutor executor = new ConfigSQLExecutor(dbContext.getSqlFilepath());
					org.frameworkset.persitent.util.SQLInfo sqlInfo = executor.getSqlInfo(getSourceDBName(),dbContext.getSqlName());
					this.executor = executor;
					dbContext.setSql(sqlInfo.getSql());
				}
				catch (SQLException e){
					throw new ESDataImportException(e);
				}

		}
		if(dbContext.getSql() != null && !dbContext.getSql().equals("")) {

			initSQLInfoParams();
		}

	}
	private void initSQLInfoParams(){
		String originSQL = dbContext.getSql();
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
				if(paramSize == 0){
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

	public String getLastValueVarName(){
		return this.sqlInfo != null?this.sqlInfo.getLastValueVarName():null;
	}




	private void commonImportData(ResultSetHandler resultSetHandler) throws Exception {
		String sourceDBName = getSourceDBName();
		boolean isEnableDBTransaction = importContext.getDbConfig() != null?importContext.getDbConfig().isEnableDBTransaction():false;
		if(importContext.getDataRefactor() == null || !isEnableDBTransaction){
			if (executor == null) {
				SQLExecutor.queryWithDBNameByNullRowHandler(resultSetHandler, sourceDBName, dbContext.getSql());
			} else {
				executor.queryWithDBNameByNullRowHandler(resultSetHandler, sourceDBName, dbContext.getSqlName());
			}
		}
		else {

			TransactionManager transactionManager = new TransactionManager();
			try {
				transactionManager.begin(TransactionManager.RW_TRANSACTION);
				if (executor == null) {
					SQLExecutor.queryWithDBNameByNullRowHandler(resultSetHandler, sourceDBName, dbContext.getSql());
				} else {
					executor.queryWithDBNameByNullRowHandler(resultSetHandler, sourceDBName, dbContext.getSqlName());
				}
				transactionManager.commit();
			} finally {
				transactionManager.releasenolog();
			}
		}
	}

	private String getSourceDBName(){
		DBConfig dbConfig = importContext.getDbConfig();
		String sourceDBName  = importContext.getSourceDBName();
		if(sourceDBName == null){
			if(dbConfig != null)
				sourceDBName = dbConfig.getDbName();
		}
		if(sourceDBName == null){

			throw new ESDataImportException("DbConfig is null,please set dbname use importBuilder.setDbName(dbname) or importBuilder.setSourceDBName(dbname) and  other database configs use importBuilder," +
					"dbname and other database configs can also been configed in appliction.properties file or other config file bboss supported.");
		}
		return sourceDBName;

	}
	private void increamentImportData(ResultSetHandler resultSetHandler) throws Exception {
		String sourceDBName = getSourceDBName();
		boolean isEnableDBTransaction = importContext.getDbConfig() != null?importContext.getDbConfig().isEnableDBTransaction():false;
		if(importContext.getDataRefactor() == null || !isEnableDBTransaction){
			if (executor == null) {
				SQLExecutor.queryBeanWithDBNameByNullRowHandler(resultSetHandler, sourceDBName, dbContext.getSql(), getParamValue());
			} else {
				executor.queryBeanWithDBNameByNullRowHandler(resultSetHandler, sourceDBName, dbContext.getSqlName(), getParamValue());

			}
		}
		else {
			TransactionManager transactionManager = new TransactionManager();
			try {
				transactionManager.begin(TransactionManager.RW_TRANSACTION);
				if (executor == null) {
					SQLExecutor.queryBeanWithDBNameByNullRowHandler(resultSetHandler, sourceDBName, dbContext.getSql(), getParamValue());
				} else {
					executor.queryBeanWithDBNameByNullRowHandler(resultSetHandler, sourceDBName, dbContext.getSqlName(), getParamValue());

				}
			} finally {
				transactionManager.releasenolog();
			}
		}
	}
	public abstract BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, Status currentStatus);
	@Override
	public void doImportData( TaskContext taskContext)  throws ESDataImportException {
		ResultSetHandler resultSetHandler = new DefaultResultSetHandler( taskContext,importContext,targetImportContext,this);

		try {
			if (sqlInfo.getParamSize() == 0) {
//			if(importContext.getDataRefactor() == null || !importContext.getDbConfig().isEnableDBTransaction()){
//				if (executor == null) {
//					SQLExecutor.queryWithDBNameByNullRowHandler(resultSetHandler, importContext.getDbConfig().getDbName(), importContext.getSql());
//				} else {
//					executor.queryBeanWithDBNameByNullRowHandler(resultSetHandler, importContext.getDbConfig().getDbName(), importContext.getSqlName(), (Map) null);
//				}
//			}
//			else {
//				TransactionManager transactionManager = new TransactionManager();
//				try {
//					transactionManager.begin(TransactionManager.RW_TRANSACTION);
//					if (executor == null) {
//						SQLExecutor.queryWithDBNameByNullRowHandler(resultSetHandler, importContext.getDbConfig().getDbName(), importContext.getSql());
//					} else {
//						executor.queryBeanWithDBNameByNullRowHandler(resultSetHandler, importContext.getDbConfig().getDbName(), importContext.getSqlName(), (Map) null);
//					}
//					transactionManager.commit();
//				} finally {
//					transactionManager.releasenolog();
//				}
//			}
				commonImportData(resultSetHandler);

			} else {
				if (!isIncreamentImport()) {
					setForceStop();
				} else {
//				if(importContext.getDataRefactor() == null || !importContext.getDbConfig().isEnableDBTransaction()){
//					if (executor == null) {
//						SQLExecutor.queryBeanWithDBNameByNullRowHandler(resultSetHandler, importContext.getDbConfig().getDbName(), importContext.getSql(), getParamValue());
//					} else {
//						executor.queryBeanWithDBNameByNullRowHandler(resultSetHandler, importContext.getDbConfig().getDbName(), importContext.getSqlName(), getParamValue());
//
//					}
//				}
//				else {
//					TransactionManager transactionManager = new TransactionManager();
//					try {
//						transactionManager.begin(TransactionManager.RW_TRANSACTION);
//						if (executor == null) {
//							SQLExecutor.queryBeanWithDBNameByNullRowHandler(resultSetHandler, importContext.getDbConfig().getDbName(), importContext.getSql(), getParamValue());
//						} else {
//							executor.queryBeanWithDBNameByNullRowHandler(resultSetHandler, importContext.getDbConfig().getDbName(), importContext.getSqlName(), getParamValue());
//
//						}
//					} finally {
//						transactionManager.releasenolog();
//					}
//				}
					increamentImportData(resultSetHandler);

				}
			}
		}
		catch (ESDataImportException e){
			throw e;
		}
		catch (Exception e){
			throw new ESDataImportException(e);
		}
	}


}
