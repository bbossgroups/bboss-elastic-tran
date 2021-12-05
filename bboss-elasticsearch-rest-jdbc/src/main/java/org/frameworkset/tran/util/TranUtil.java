package org.frameworkset.tran.util;
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

import bboss.org.apache.velocity.VelocityContext;
import com.frameworkset.common.poolman.ConfigSQLExecutor;
import com.frameworkset.util.VariableHandler;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.persitent.util.GloableSQLUtil;
import org.frameworkset.persitent.util.SQLInfo;
import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.ESDataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.output.DBOutPutContext;
import org.frameworkset.tran.db.output.TranSQLInfo;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.annotations.DateFormateMeta;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/18 0:18
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class TranUtil {
	public static final String lineSeparator;
	static{
		lineSeparator = java.security.AccessController.doPrivileged(
				new sun.security.action.GetPropertyAction("line.separator"));
	}
/**
	public static void initSQLInfo(DBOutPutContext dbContext, ImportContext importContext) throws ESDataImportException {
		TranSQLInfo sqlInfo = new TranSQLInfo();

		ConfigSQLExecutor configSQLExecutor = new ConfigSQLExecutor(dbContext.getSqlFilepath());

		try {
			SQLInfo sqlinfo = configSQLExecutor.getSqlInfo(importContext.getDbConfig().getDbName(), dbContext.getSqlName());
			sqlInfo.setOriginSQL(sqlinfo.getSql());
			String sql = parserSQL(  sqlinfo);

			VariableHandler.SQLStruction sqlstruction = sqlinfo.getSqlutil().getSQLStruction(sqlinfo,sql);
			sql = sqlstruction.getSql();
			sqlInfo.setSql(sql);
			List<VariableHandler.Variable> vars = sqlstruction.getVariables();
			sqlInfo.setVars(vars);
			dbContext.setSqlInfo(sqlInfo);
		} catch (SQLException e) {
			throw new ESDataImportException("Init SQLInfo failed",e);
		}


	}*/

	private static  TranSQLInfo buildTranSQLInfo(String sqlName,boolean issql,String sqlFilepath, String dbname){
		TranSQLInfo sqlInfo = new TranSQLInfo();
		SQLInfo sqlinfo = null;
		ConfigSQLExecutor configSQLExecutor = null;
		try {
			if(issql) {

				if(sqlName != null)
					sqlinfo = GloableSQLUtil.getGlobalSQLUtil().getSQLInfo(sqlName);
			}
			else{
				configSQLExecutor = new ConfigSQLExecutor(sqlFilepath);
				sqlinfo = configSQLExecutor.getSqlInfo(dbname, sqlName);
			}

			if(sqlinfo == null){
				return null;
			}



			sqlInfo.setOriginSQL(sqlinfo.getSql());
			String sql = parserSQL(  sqlinfo);

			VariableHandler.SQLStruction sqlstruction = sqlinfo.getSqlutil().getSQLStruction(sqlinfo,sql);
			sql = sqlstruction.getSql();
			sqlInfo.setSql(sql);
			List<VariableHandler.Variable> vars = sqlstruction.getVariables();
			sqlInfo.setVars(vars);
			return sqlInfo;
		} catch (SQLException e) {
			throw new ESDataImportException("Init TargetSQLInfo failed",e);
		}
	}
//	private static void assertNull(DBOutPutContext dbContext, DBConfig db){
//		if(dbContext.getTargetSqlInfo() == null && dbContext.getTargetDeleteSqlInfo() == null && dbContext.getTargetDeleteSqlInfo() == null )
//			throw new ESDataImportException("Init TargetSQLInfo  failed:InsertSqlName="+dbContext.getInsertSqlName() + " and insertSql = "+dbContext.getInsertSql());
//	}
	public static void initTargetSQLInfo(DBOutPutContext dbContext, String db) throws ESDataImportException {
		TranSQLInfo sqlInfo = null;
		SQLInfo sqlinfo = null;
		String sqlName = dbContext.getInsertSqlName();

		if(sqlName == null) {
			sqlName = dbContext.getInsertSql();
			sqlInfo = buildTranSQLInfo( sqlName,true,  dbContext.getSqlFilepath(),   db);


		}
		else{
			sqlInfo = buildTranSQLInfo( sqlName,false,  dbContext.getSqlFilepath(),   db);
		}

		if(sqlInfo != null){
			dbContext.setTargetSqlInfo(sqlInfo);
			sqlInfo = null;
		}

		sqlName = dbContext.getUpdateSqlName();

		if(sqlName == null) {
			sqlName = dbContext.getUpdateSql();
			sqlInfo = buildTranSQLInfo( sqlName,true,  dbContext.getSqlFilepath(),   db);


		}
		else{

			sqlInfo = buildTranSQLInfo( sqlName,false,  dbContext.getSqlFilepath(),   db);
		}

		if(sqlInfo != null){
			dbContext.setTargetUpdateSqlInfo(sqlInfo);
			sqlInfo = null;
		}
		sqlName = dbContext.getDeleteSqlName();

		if(sqlName == null) {
			sqlName = dbContext.getDeleteSql();
			sqlInfo = buildTranSQLInfo( sqlName,true,  dbContext.getSqlFilepath(),   db);


		}
		else{

			sqlInfo = buildTranSQLInfo( sqlName,false,  dbContext.getSqlFilepath(),   db);
		}

		if(sqlInfo != null){
			dbContext.setTargetDeleteSqlInfo(sqlInfo);
			sqlInfo = null;
		}
//		assertNull(dbContext,  db);


	}

	public static void initTaskContextSQLInfo(TaskContext taskContext,ImportContext importContext,
			 									ImportContext targetImportContext){
		if(taskContext != null && taskContext.getDbmportConfig() != null) {
			String dbName = null;
			if(taskContext.getTargetDBConfig() != null){
				DBConfig dbConfig = taskContext.getTargetDBConfig();
				if(dbConfig != null)
					dbName = dbConfig.getDbName();
			}
			if(dbName == null) {
				if (targetImportContext != null && targetImportContext instanceof DBOutPutContext) {
					DBConfig dbConfig = ((DBOutPutContext) targetImportContext).getTargetDBConfig(taskContext);
					if(dbConfig != null)
						dbName = dbConfig.getDbName();
				}
			}
			if(dbName == null){
				DBConfig dbConfig = importContext.getDbConfig();
				if(dbConfig != null)
					dbName = dbConfig.getDbName();

			}
			if(dbName != null)
				TranUtil.initTargetSQLInfo(taskContext, dbName);
		}
	}
	public static void initTargetSQLInfo(TaskContext dbContext, String db) throws ESDataImportException {
		if(dbContext == null)
			return;
		ImportContext targetImportContext = dbContext.getTargetImportContext();
		if(!(targetImportContext instanceof DBOutPutContext)){
			return ;
		}
		TranSQLInfo sqlInfo = null;
		SQLInfo sqlinfo = null;
		String sqlName = dbContext.getInsertSqlName();
		String sqlFilePath = dbContext.getSqlFilepath();
		if(sqlFilePath == null){

			sqlFilePath = ((DBOutPutContext)targetImportContext).getSqlFilepath();

		}
		if(sqlName == null) {
			sqlName = dbContext.getInsertSql();
			if(sqlName != null)
				sqlInfo = buildTranSQLInfo( sqlName,true,  sqlFilePath,   db);


		}
		else{
			sqlInfo = buildTranSQLInfo( sqlName,false,  sqlFilePath,   db);
		}

		if(sqlInfo != null){
			dbContext.setTargetSqlInfo(sqlInfo);
			sqlInfo = null;
		}

		sqlName = dbContext.getUpdateSqlName();

		if(sqlName == null) {
			sqlName = dbContext.getUpdateSql();
			if(sqlName != null)
				sqlInfo = buildTranSQLInfo( sqlName,true,  sqlFilePath,   db);


		}
		else{
			sqlInfo = buildTranSQLInfo( sqlName,false,  sqlFilePath,   db);
		}

		if(sqlInfo != null){
			dbContext.setTargetUpdateSqlInfo(sqlInfo);
			sqlInfo = null;
		}
		sqlName = dbContext.getDeleteSqlName();

		if(sqlName == null) {
			sqlName = dbContext.getDeleteSql();
			if(sqlName != null)
				sqlInfo = buildTranSQLInfo( sqlName,true,  sqlFilePath,   db);


		}
		else{
			sqlInfo = buildTranSQLInfo( sqlName,false,  sqlFilePath,   db);
		}

		if(sqlInfo != null){
			dbContext.setTargetDeleteSqlInfo(sqlInfo);
			sqlInfo = null;
		}
//		assertNull(dbContext,  db);


	}
//
//	public static ConfigSQLExecutor initTargetSQLInfo(DBOutPutContext dbContext, DBConfig db) throws ESDataImportException {
//		TranSQLInfo sqlInfo = new TranSQLInfo();
//		SQLInfo sqlinfo = null;
//		String sqlName = dbContext.getInsertSqlName();
//		ConfigSQLExecutor configSQLExecutor = null;
//		try {
//			if(sqlName == null) {
//				sqlName = dbContext.getInsertSql();
//				if(sqlName != null)
//					sqlinfo = GloableSQLUtil.getGlobalSQLUtil().getSQLInfo(sqlName);
//			}
//			else{
//				configSQLExecutor = new ConfigSQLExecutor(dbContext.getSqlFilepath());
//				sqlinfo = configSQLExecutor.getSqlInfo(db.getDbName(), sqlName);
//			}
//
//			if(sqlinfo == null){
//				throw new ESDataImportException("Init TargetSQLInfo failed:InsertSqlName="+dbContext.getInsertSqlName() + " and insertSql = "+dbContext.getInsertSql());
//			}
//
//
//
//			sqlInfo.setOriginSQL(sqlinfo.getSql());
//			String sql = parserSQL(  sqlinfo);
//
//			VariableHandler.SQLStruction sqlstruction = sqlinfo.getSqlutil().getSQLStruction(sqlinfo,sql);
//			sql = sqlstruction.getSql();
//			sqlInfo.setSql(sql);
//			List<VariableHandler.Variable> vars = sqlstruction.getVariables();
//			sqlInfo.setVars(vars);
//			dbContext.setTargetSqlInfo(sqlInfo);
//		} catch (SQLException e) {
//			throw new ESDataImportException("Init TargetSQLInfo failed",e);
//		}
//		return configSQLExecutor;
//
//
//	}
	private static VelocityContext buildVelocityContext()
	{


		VelocityContext context_ = new VelocityContext();

//		com.frameworkset.common.poolman.Param temp = null;
//		if(sqlparams != null && sqlparams.size()>0)
//		{
//
//			Iterator<Map.Entry<String, com.frameworkset.common.poolman.Param>> it = sqlparams.entrySet().iterator();
//			while(it.hasNext())
//			{
//				Map.Entry<String, com.frameworkset.common.poolman.Param> entry = it.next();
//				temp = entry.getValue();
//
//				if(!temp.getType().equals(NULL))
//					context_.put(entry.getKey(), temp.getData());
//			}
//		}
		return context_;

	}
	public static String parserSQL(org.frameworkset.persitent.util.SQLInfo sqlinfo){
		String sql = null;
		if(sqlinfo.istpl())
		{
			sqlinfo.getSqltpl().process();//识别sql语句是不是真正的velocity sql模板
			if(sqlinfo.istpl())
			{
				VelocityContext vcontext = buildVelocityContext();//一个context是否可以被同时用于多次运算呢？

				BBossStringWriter sw = new BBossStringWriter();
				sqlinfo.getSqltpl().merge(vcontext,sw);
				sql = sw.toString();
			}
			else
			{
				sql = sqlinfo.getSql();
			}

		}
		else
		{
			sql = sqlinfo.getSql();
		}
		return sql;
	}
	public static Date getDateTimeValue(String colName, Object value, ImportContext importContext) throws ESDataImportException {
		if(value == null)
			return null;
		if(value instanceof Date)
			return (Date)value;
		else if(value instanceof Long ){
			return new Date((Long)value);
		}
		else if(value instanceof String){
			DateFormat dateFormat = null;
			if(importContext.getDateFormat()!=null){
				DateFormateMeta dateFormateMeta = DateFormateMeta.buildDateFormateMeta(importContext.getDateFormat(),importContext.getLocale(),importContext.getTimeZone());
				dateFormat = dateFormateMeta.toDateFormat();

			}
			else{
				dateFormat = SerialUtil.getDateFormateMeta().toDateFormat();
			}
			try {
				return dateFormat.parse((String)value);
			} catch (ParseException e) {
				throw new ESDataImportException("Illegment colName["+colName+"] date value:"+(String)value,e);
			}
		}
		else{
			throw new ESDataImportException("Illegment colName["+colName+"] date value:"+(String)value);
		}
	}

}
