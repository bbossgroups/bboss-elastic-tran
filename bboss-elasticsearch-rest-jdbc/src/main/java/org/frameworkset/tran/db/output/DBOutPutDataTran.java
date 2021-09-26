package org.frameworkset.tran.db.output;

import com.frameworkset.util.VariableHandler;
import org.frameworkset.elasticsearch.ElasticSearchException;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.DBRecord;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.ParallImportCount;
import org.frameworkset.tran.metrics.SerialImportCount;
import org.frameworkset.tran.record.SplitKeys;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCall;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class DBOutPutDataTran extends BaseDataTran {
	protected DBOutPutContext es2DBContext ;
	@Override
	public void logTaskStart(Logger logger) {
//		StringBuilder builder = new StringBuilder().append("import data to db[").append(importContext.getDbConfig().getDbUrl())
//				.append("] dbuser[").append(importContext.getDbConfig().getDbUser())
//				.append("] insert sql[").append(es2DBContext.getTargetSqlInfo() == null ?"": es2DBContext.getTargetSqlInfo().getOriginSQL()).append("] \r\nupdate sql[")
//					.append(es2DBContext.getTargetUpdateSqlInfo() == null?"":es2DBContext.getTargetUpdateSqlInfo().getOriginSQL()).append("] \r\ndelete sql[")
//					.append(es2DBContext.getTargetDeleteSqlInfo() == null ?"":es2DBContext.getTargetDeleteSqlInfo().getOriginSQL()).append("] start.");
		logger.info(taskInfo + " start.");
	}
	private String taskInfo ;
	public void init(){
		super.init();
		es2DBContext = targetImportContext == null ?(DBOutPutContext)importContext:(DBOutPutContext)targetImportContext;
		DBConfig dbConfig = null;
		if(es2DBContext.getTargetDBConfig() == null)
			dbConfig = importContext.getDbConfig();
		else
			dbConfig = es2DBContext.getTargetDBConfig();
		StringBuilder builder = new StringBuilder().append("Import data to db[").append(dbConfig.getDbUrl())
				.append("] dbuser[").append(dbConfig.getDbUser()).append("]");
		if(es2DBContext.getTargetSqlInfo() != null ) {
			builder.append(" insert sql[").append( es2DBContext.getTargetSqlInfo().getOriginSQL()).append("]");
		}
		if(es2DBContext.getTargetUpdateSqlInfo() != null ) {
			builder.append("\r\nupdate sql[")
					.append(es2DBContext.getTargetUpdateSqlInfo().getOriginSQL()).append("]");
		}
		if(es2DBContext.getTargetDeleteSqlInfo() != null ) {
			builder.append("\r\ndelete sql[")
					.append(es2DBContext.getTargetDeleteSqlInfo().getOriginSQL()).append("]");
		}
		taskInfo = builder.toString();
	}


	public DBOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext,Status currentStatus) {
		super(   taskContext,jdbcResultSet,importContext, targetImportContext,  currentStatus);
	}


	public String serialExecute(  ){
		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
//		Status currentStatus = importContext.getCurrentStatus();
		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		ImportCount importCount = new SerialImportCount();
		int taskNo = 0;
		long totalCount = 0;
		long ignoreTotalCount = 0;
		boolean reachEOFClosed = false;
		try {

			//		GetCUDResult CUDResult = null;
			Object temp = null;
			Param param = null;
			List<DBRecord> records = new ArrayList<DBRecord>();
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					if(records.size() > 0) {
						taskNo ++;
						TaskCommand<List<DBRecord>, String> taskCommand = new Base2DBTaskCommandImpl( importCount, importContext,targetImportContext, records,
								taskNo, importCount.getJobNo(),taskInfo,true,lastValue,   currentStatus,reachEOFClosed);
						TaskCall.call(taskCommand);
//						importContext.flushLastValue(lastValue);
						records.clear();
						if(isPrintTaskLog()) {
							long end = System.currentTimeMillis();
							logger.info(new StringBuilder().append("Serial import Force flush records Take time:").append((end - start)).append("ms")
									.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
									.append(importCount.getIgnoreTotalCount()).append(" records.").toString());

						}

					}
//					importContext.flushLastValue(lastValue);
					if(isPrintTaskLog()) {

						long end = System.currentTimeMillis();
						logger.info(new StringBuilder().append("Serial import Force flush datas Take time:").append((end - start)).append("ms")
								.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
								.append(ignoreTotalCount).append(" records.").toString());

					}
					continue;
				}
				else if(!hasNext.booleanValue()){
					break;
				}
				try {
					if (lastValue == null)
						lastValue = importContext.max(currentValue, getLastValue());
					else {
						lastValue = importContext.max(lastValue, getLastValue());
					}
//					Context context = new ContextImpl(importContext, jdbcResultSet, null);
					Context context = importContext.buildContext(taskContext,jdbcResultSet, null);
					if(!reachEOFClosed)
						reachEOFClosed = context.reachEOFClosed();
					if(context.removed()){
						if(!reachEOFClosed)//如果是文件末尾，那么是空行记录，不需要记录忽略信息，
							importCount.increamentIgnoreTotalCount();
						else
							importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
						continue;
					}
					context.refactorData();
					context.afterRefactor();
					if (context.isDrop()) {
						importCount.increamentIgnoreTotalCount();
						continue;
					}
					DBRecord record = buildRecord(  context );

					records.add(record);
					//						evalBuilk(this.jdbcResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
					totalCount++;
				} catch (Exception e) {
					throw new ElasticSearchException(e);
				}
			}
			if(records.size() > 0) {
				taskNo ++;
				TaskCommand<List<DBRecord>, String> taskCommand = new Base2DBTaskCommandImpl(importCount, importContext, targetImportContext,records,
						taskNo, importCount.getJobNo(),taskInfo,true,lastValue,   currentStatus,reachEOFClosed);
				TaskCall.call(taskCommand);
//				importContext.flushLastValue(lastValue);
			}
			if(isPrintTaskLog()) {
				long end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Serial import All Take time:").append((end - start)).append("ms")
						.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
						.append(importCount.getIgnoreTotalCount()).append(" records.").toString());

			}
		}
		catch (ElasticSearchException e){
			exception = e;
			throw e;


		}
		catch (Exception e){
			exception = e;
			throw new ElasticSearchException(e);


		} finally {

			if(!TranErrorWrapper.assertCondition(exception ,importContext)){
				if(!importContext.getDataTranPlugin().isMultiTran()) {
					this.stop();
				} else{
					this.stopTranOnly();
				}
			}
			if(importContext.isCurrentStoped()){
				this.stopTranOnly();
			}
			importCount.setJobEndTime(new Date());
		}
		return null;

	}
	private DBRecord buildRecord(Context context){
		List<VariableHandler.Variable> vars = null;
		Object temp = null;
		Param param = null;

		DBRecord dbRecord = new DBRecord();
		TranSQLInfo insertSqlinfo = es2DBContext.getTargetSqlInfo();
		TranSQLInfo updateSqlinfo = es2DBContext.getTargetUpdateSqlInfo();
		TranSQLInfo deleteSqlinfo = es2DBContext.getTargetDeleteSqlInfo();
		if(context.isInsert()) {
			dbRecord.setAction(DBRecord.INSERT);
			vars = insertSqlinfo.getVars();
		}
		else if(context.isUpdate()) {
			dbRecord.setAction(DBRecord.UPDATE);
			vars = updateSqlinfo.getVars();
		}
		else {
			dbRecord.setAction(DBRecord.DELETE);
			vars = deleteSqlinfo.getVars();
		}
		Object  keys = jdbcResultSet.getKeys();
		String[] splitColumns = null;
		if(keys != null) {
			boolean isSplitKeys = keys instanceof SplitKeys;
			if(isSplitKeys) {
				SplitKeys splitKeys = (SplitKeys) keys;

				splitColumns = splitKeys.getSplitKeys();
			}

		}
		List<Param> record = new ArrayList<Param>();
		Map<String,Object> addedFields = new HashMap<String,Object>();
		//context优先级高于splitColumns,splitColumns高于全局配置，全局配置高于数据源级别字段值
		List<FieldMeta> fieldValueMetas = context.getFieldValues();

		appendFieldValues( record, vars,    fieldValueMetas,  addedFields);
		//计算记录切割字段
		appendSplitFieldValues(dbRecord,
				splitColumns,
				addedFields,context);
		fieldValueMetas = context.getESJDBCFieldValues();
		appendFieldValues(  record, vars,   fieldValueMetas,  addedFields);
		String varName = null;
		for(int i = 0;i < vars.size(); i ++)
		{
			VariableHandler.Variable var = vars.get(i);

			varName = var.getVariableName();
			FieldMeta fieldMeta = context.getMappingName(varName);
			if(fieldMeta != null) {
				if(fieldMeta.getIgnore() != null && fieldMeta.getIgnore() == true)
					continue;
				varName = fieldMeta.getEsFieldName();
			}
			if(addedFields.get(varName) != null)
				continue;
			temp = jdbcResultSet.getValue(varName);
			if(temp == null) {
				if(logger.isWarnEnabled())
					logger.warn("未指定绑定变量的值：{}",var.getVariableName());
			}
			param = new Param();
			param.setVariable(var);
			param.setIndex(var.getPosition()  +1);
			param.setValue(temp);
			param.setName(var.getVariableName());
			record.add(param);

		}

		dbRecord.setParams(record);
		return dbRecord;
	}
	@Override
	public String parallelBatchExecute() {
		int count = 0;
		ExecutorService service = importContext.buildThreadPool();
		List<Future> tasks = new ArrayList<Future>();
		int taskNo = 0;
		ImportCount totalCount = new ParallImportCount();
		Exception exception = null;
//		Status currentStatus = importContext.getCurrentStatus();
		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		Object lastValue = null;
		TranErrorWrapper tranErrorWrapper = new TranErrorWrapper(importContext);
		int batchsize = importContext.getStoreBatchSize();
		boolean reachEOFClosed = false;
		try {
			TranSQLInfo sqlinfo = es2DBContext.getTargetSqlInfo();
			Object temp = null;
			Param param = null;
			List<DBRecord> records = new ArrayList<DBRecord>();
			while (true) {
				if(!tranErrorWrapper.assertCondition()) {
					tranErrorWrapper.throwError();
				}
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					if(count > 0) {//强制刷新数据
						count = 0;
						taskNo++;
						Base2DBTaskCommandImpl taskCommand = new Base2DBTaskCommandImpl( totalCount, importContext, targetImportContext,records,
								taskNo, totalCount.getJobNo(),taskInfo,false,lastValue,  currentStatus,reachEOFClosed);
						records = new ArrayList<DBRecord>();
						tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
					}
					continue;
				}
				else if(!hasNext.booleanValue()){
					break;
				}
				if(lastValue == null)
					lastValue = importContext.max(currentValue,getLastValue());
				else{
					lastValue = importContext.max(lastValue,getLastValue());
				}

//				Context context = new ContextImpl(importContext, jdbcResultSet, null);
				Context context = importContext.buildContext(taskContext,jdbcResultSet, null);

				if(!reachEOFClosed)
					reachEOFClosed = context.reachEOFClosed();
				if(context.removed()){
					if(!reachEOFClosed)//如果是文件末尾，那么是空行记录，不需要记录忽略信息，
						totalCount.increamentIgnoreTotalCount();
					else
						importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
					continue;
				}
				context.refactorData();
				context.afterRefactor();
				if (context.isDrop()) {
					totalCount.increamentIgnoreTotalCount();
					continue;
				}
				DBRecord record = buildRecord(  context);
				records.add(record);
				//						evalBuilk(this.jdbcResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
				count++;
				if (count >= batchsize) {

					count = 0;
					taskNo ++;
					Base2DBTaskCommandImpl taskCommand = new Base2DBTaskCommandImpl(totalCount,importContext,targetImportContext,records,taskNo,
							totalCount.getJobNo(),taskInfo,false,lastValue,  currentStatus,reachEOFClosed);
					records = new ArrayList<DBRecord>();
					tasks.add(service.submit(new TaskCall(taskCommand,  tranErrorWrapper)));



				}

			}
			if (count > 0) {
				if(!tranErrorWrapper.assertCondition()) {
					tranErrorWrapper.throwError();
				}
//				if(this.error != null && !importContext.isContinueOnError()) {
//					throw error;
//				}
				taskNo ++;
				Base2DBTaskCommandImpl taskCommand = new Base2DBTaskCommandImpl(totalCount,importContext,targetImportContext,
						records,taskNo,totalCount.getJobNo(),taskInfo,false,lastValue,  currentStatus,reachEOFClosed);
				tasks.add(service.submit(new TaskCall(taskCommand,tranErrorWrapper)));

				if(isPrintTaskLog())
					logger.info(new StringBuilder().append("Pararrel batchsubmit tasks:").append(taskNo).toString());
			}
			else{
				if(isPrintTaskLog())
					logger.info(new StringBuilder().append("Pararrel batchsubmit tasks:").append(taskNo).toString());
			}

		} catch (SQLException e) {
			exception = e;
			throw new ElasticSearchException(e);

		} catch (ElasticSearchException e) {
			exception = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			throw new ElasticSearchException(e);
		}
		finally {
			waitTasksComplete(   tasks,  service,exception,  lastValue,totalCount ,tranErrorWrapper,(WaitTasksCompleteCallBack)null,reachEOFClosed);
			totalCount.setJobEndTime(new Date());
		}

		return null;
	}

	@Override
	public String batchExecute() {
		int count = 0;
		String ret = null;
		int taskNo = 0;
		Exception exception = null;
//		Status currentStatus = importContext.getCurrentStatus();
		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		Object lastValue = null;
		TranErrorWrapper tranErrorWrapper = new TranErrorWrapper(importContext);
		long start = System.currentTimeMillis();
		long istart = 0;
		long end = 0;
		long totalCount = 0;
		long ignoreTotalCount = 0;
		ImportCount importCount = new SerialImportCount();
		int batchsize = importContext.getStoreBatchSize();
		boolean reachEOFClosed = false;
		try {
			istart = start;

			List<DBRecord> records = new ArrayList<DBRecord>();
			while (true) {
				if(!tranErrorWrapper.assertCondition()) {
					jdbcResultSet.stop();
					tranErrorWrapper.throwError();
				}
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					if(count > 0) {//强制flush数据
						taskNo++;
						Base2DBTaskCommandImpl taskCommand = new Base2DBTaskCommandImpl(importCount, importContext, targetImportContext,records,
								taskNo, importCount.getJobNo(),taskInfo,false,lastValue,  currentStatus,reachEOFClosed);
						int temp = count;
						count = 0;
						records = new ArrayList<DBRecord>();
						ret = TaskCall.call(taskCommand);
//						importContext.flushLastValue(lastValue);

						if (isPrintTaskLog()) {
							end = System.currentTimeMillis();
							logger.info(new StringBuilder().append("Batch import Force flush datas Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
									.append(",import ").append(temp).append(" records.").toString());
							istart = end;
						}

						totalCount += temp;
					}
					continue;
				}
				else if(!hasNext.booleanValue()){
					break;
				}
				if(lastValue == null)
					lastValue = importContext.max(currentValue,getLastValue());
				else{
					lastValue = importContext.max(lastValue,getLastValue());
				}
//				Context context = new ContextImpl(importContext, jdbcResultSet, null);
				Context context = importContext.buildContext(taskContext,jdbcResultSet, null);

				if(!reachEOFClosed)
					reachEOFClosed = context.reachEOFClosed();
				if(context.removed()){
					if(!reachEOFClosed)//如果是文件末尾，那么是空行记录，不需要记录忽略信息，
						importCount.increamentIgnoreTotalCount();
					else
						importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
					continue;
				}
				context.refactorData();

				context.afterRefactor();
				if (context.isDrop()) {
					importCount.increamentIgnoreTotalCount();
					continue;
				}
				DBRecord record = buildRecord(  context );
				records.add(record);
				count++;
				if (count >= batchsize) {

					taskNo ++;
					Base2DBTaskCommandImpl taskCommand = new Base2DBTaskCommandImpl(importCount,importContext,targetImportContext,records,taskNo,
							importCount.getJobNo(),taskInfo,false,lastValue,  currentStatus,reachEOFClosed);
					int temp  = count;
					count = 0;
					records = new ArrayList<DBRecord>();
					ret = TaskCall.call(taskCommand);
//					importContext.flushLastValue(lastValue);

					if(isPrintTaskLog())  {
						end = System.currentTimeMillis();
						logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
								.append(",import ").append(temp).append(" records.").toString());
						istart = end;
					}

					totalCount += temp;


				}

			}
			if (count > 0) {
				if(!tranErrorWrapper.assertCondition()) {
					tranErrorWrapper.throwError();
				}
				taskNo ++;
				Base2DBTaskCommandImpl taskCommand = new Base2DBTaskCommandImpl(importCount,importContext,targetImportContext,
						records,taskNo,importCount.getJobNo(),taskInfo,false,lastValue,  currentStatus,reachEOFClosed);
				ret = TaskCall.call(taskCommand);
//				importContext.flushLastValue(lastValue);
				if(isPrintTaskLog())  {
					end = System.currentTimeMillis();
					logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
							.append(",import ").append(count).append(" records.").toString());

				}
				totalCount += count;
			}
			if(isPrintTaskLog()) {
				end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Batch import Execute Tasks:").append(taskNo).append(",All Take time:").append((end - start)).append("ms")
						.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
						.append(ignoreTotalCount).append(" records.").toString());

			}
		}  catch (ElasticSearchException e) {
			exception = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			throw new ElasticSearchException(e);
		}
		finally {
			if(!tranErrorWrapper.assertCondition(exception)){
				if(!importContext.getDataTranPlugin().isMultiTran()) {
					this.stop();
				} else{
					this.stopTranOnly();
				}
			}
			importCount.setJobEndTime(new Date());
		}

		return ret;
	}


	private void appendFieldValues(List<Param> record,
			List<VariableHandler.Variable> vars,
			List<FieldMeta> fieldValueMetas,
			Map<String, Object> addedFields) {
		if(fieldValueMetas ==  null || fieldValueMetas.size() == 0){
			return;
		}
		int i = 0;
		Param param = null;
		for(VariableHandler.Variable variable:vars){
			if(addedFields.containsKey(variable.getVariableName()))
				continue;
			for(FieldMeta fieldMeta:fieldValueMetas){
				if(variable.getVariableName().equals(fieldMeta.getEsFieldName())){
					param = new Param();
					param.setVariable(variable);
					param.setIndex(variable.getPosition() +1);
					param.setValue(fieldMeta.getValue());
					param.setName(variable.getVariableName());
					record.add(param);
//					statement.setObject(i +1,fieldMeta.getValue());
					addedFields.put(variable.getVariableName(),dummy);
					break;
				}
			}
		}
	}




}
