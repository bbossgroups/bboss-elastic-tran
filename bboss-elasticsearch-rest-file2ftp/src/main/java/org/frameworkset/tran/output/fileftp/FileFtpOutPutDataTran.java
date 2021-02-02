package org.frameworkset.tran.output.fileftp;

import com.frameworkset.orm.annotation.BatchContext;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.ElasticSearchException;
import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.ParallImportCount;
import org.frameworkset.tran.metrics.SerialImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.task.TaskCall;
import org.slf4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class FileFtpOutPutDataTran extends BaseDataTran {
	protected FileFtpOupputContext fileFtpOupputContext ;
//	protected String fileName;
//	protected String remoteFileName;
	protected FileUtil fileUtil;
	protected String path;
	protected int fileSeq = 1;
	protected String lineSeparator;
	protected CountDownLatch countDownLatch;
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
	private FileUtil initFileUtil(){
		path = fileFtpOupputContext.getFileDir();
		String name = fileFtpOupputContext.generateFileName(  fileSeq);
		String fileName = SimpleStringUtil.getPath(path,name);
		String remoteFileName = SimpleStringUtil.getPath(fileFtpOupputContext.getRemoteFileDir(),name);
		fileSeq ++;
		StringBuilder builder = new StringBuilder().append("Import data to ftp ip[").append(fileFtpOupputContext.getFtpIP())
				.append("] ftp user[").append(fileFtpOupputContext.getFtpUser())
				.append("] ftp password[").append(fileFtpOupputContext.getFtpPassword()).append("] ftp port[")
				.append(fileFtpOupputContext.getFtpPort()).append("] file [")
				.append(fileName).append("]");
		taskInfo = builder.toString();

		try {
			FileUtil fileUtil = new FileUtil(taskInfo,fileFtpOupputContext,path,fileName,remoteFileName,fileFtpOupputContext.getFileWriterBuffsize());
			return fileUtil;
		} catch (IOException e) {
			throw new ESDataImportException("init file writer failed:"+fileName,e);
		}

	}
	public void init(){
		super.init();
		lineSeparator = java.security.AccessController.doPrivileged(
				new sun.security.action.GetPropertyAction("line.separator"));
		fileFtpOupputContext = (FileFtpOupputContext)targetImportContext;

		fileUtil = initFileUtil();

	}
	@Override
	public String tran() throws ESDataImportException {
		try {
			String ret = super.tran();
			fileUtil.close();
			fileUtil = null;
			return ret;
		}
		finally {
			if(this.countDownLatch != null)
				countDownLatch.countDown();
		}
	}

	public FileFtpOutPutDataTran(TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, CountDownLatch countDownLatch) {
		super(jdbcResultSet,importContext, targetImportContext);
		this.countDownLatch = countDownLatch;
	}
	private CommonRecord buildRecord(Context context){
		String[] columns = targetImportContext.getExportColumns();
		if (columns == null){
			Set<String> keys = jdbcResultSet.getKeys();
			columns = keys.toArray(new String[keys.size()]);
		}
		Object temp = null;
		CommonRecord dbRecord = new CommonRecord();

		Map<String,Object> addedFields = new HashMap<String,Object>();

		List<FieldMeta> fieldValueMetas = context.getFieldValues();//context优先级高于，全局配置，全局配置高于字段值

		appendFieldValues( dbRecord, columns,    fieldValueMetas,  addedFields);
		fieldValueMetas = context.getESJDBCFieldValues();
		appendFieldValues(  dbRecord, columns,   fieldValueMetas,  addedFields);
		String varName = null;
		for(int i = 0;i < columns.length; i ++)
		{
			varName = columns[i];
			if(addedFields.get(varName) != null)
				continue;
			FieldMeta fieldMeta = context.getMappingName(varName);
			if(fieldMeta != null) {
				if(fieldMeta.getIgnore() != null && fieldMeta.getIgnore() == true)
					continue;
				varName = fieldMeta.getEsFieldName();
			}
			temp = jdbcResultSet.getValue(varName);
			if(temp == null) {
				if(logger.isWarnEnabled())
					logger.warn("未指定绑定变量的值：{}",varName);
			}
			dbRecord.addData(varName,temp);


		}


		return dbRecord;
	}

	public String serialExecute(){
		StringBuilder builder = new StringBuilder();
		BBossStringWriter writer = new BBossStringWriter(builder);
		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
		Status currentStatus = importContext.getCurrentStatus();
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		ImportCount importCount = new SerialImportCount();
		int taskNo = 0;
		long totalCount = 0;
		long ignoreTotalCount = 0;

		try {

			//		GetCUDResult CUDResult = null;
			Object temp = null;
			Param param = null;
//			List<DBRecord> records = new ArrayList<DBRecord>();
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					String ret = null;
					if(builder.length() > 0) {
						String _dd =  builder.toString();
						builder.setLength(0);
						FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
								totalCount, taskNo, importCount.getJobNo(),fileUtil,lastValue);
						taskCommand.setDatas(_dd);
						ret = TaskCall.call(taskCommand);
						taskNo ++;
					}
					else{
						ret = "{\"took\":0,\"errors\":false}";
					}
//					importContext.flushLastValue(lastValue);
					if(isPrintTaskLog()) {

						long end = System.currentTimeMillis();
						logger.info(new StringBuilder().append("Force flush datas Take time:").append((end - start)).append("ms")
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
					Context context = importContext.buildContext(jdbcResultSet, null);
					context.refactorData();
					context.afterRefactor();
					if (context.isDrop()) {
						importCount.increamentIgnoreTotalCount();
						continue;
					}
					CommonRecord record = buildRecord(  context );
					if(builder.length() > 0){
						builder.append(lineSeparator);

					}
					fileFtpOupputContext.generateReocord(context,record, writer);
//					fileUtil.writeData(fileFtpOupputContext.generateReocord(record));
//					//						evalBuilk(this.jdbcResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
					totalCount++;
					if(fileFtpOupputContext.getMaxFileRecordSize() > 0 && totalCount > 0
							&& (totalCount % fileFtpOupputContext.getMaxFileRecordSize() == 0)){//reached max file record size
						String _dd =  builder.toString();
						builder.setLength(0);
						FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
								totalCount, taskNo, importCount.getJobNo(),fileUtil,lastValue);
						taskCommand.setDatas(_dd);
						TaskCall.call(taskCommand);
						taskNo ++;
						fileUtil.sendFile(fileFtpOupputContext);
						fileUtil = this.initFileUtil();
					}
				} catch (Exception e) {
					throw new DataImportException(e);
				}
			}
			if(builder.length() > 0) {

				FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
						totalCount, taskNo, importCount.getJobNo(),fileUtil,lastValue);
				taskNo ++;
				taskCommand.setDatas(builder.toString());
				builder.setLength(0);
				TaskCall.call(taskCommand);

//				importContext.flushLastValue(lastValue);
				fileUtil.sendFile(fileFtpOupputContext);//传输文件
			}
			if(isPrintTaskLog()) {
				long end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("All Take time:").append((end - start)).append("ms")
						.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
						.append(importCount.getIgnoreTotalCount()).append(" records.").toString());

			}
		}
		catch (DataImportException e){
			exception = e;
			throw e;


		}
		catch (Exception e){
			exception = e;
			throw new DataImportException(e);


		} finally {

			if(!TranErrorWrapper.assertCondition(exception ,importContext)){
				stop();
			}
			if(importContext.isCurrentStoped()){
				stop();
			}
			importCount.setJobEndTime(new Date());
		}
		return null;

	}

	/**
	 * 并行批处理导入

	 * @return
	 */
	public String parallelBatchExecute( ){
		int count = 0;
		StringBuilder builder = new StringBuilder();
		BBossStringWriter writer = new BBossStringWriter(builder);
		String ret = null;
		ExecutorService	service = importContext.buildThreadPool();
		List<Future> tasks = new ArrayList<Future>();
		int taskNo = 0;
		ImportCount totalCount = new ParallImportCount();
		Exception exception = null;
		Status currentStatus = importContext.getCurrentStatus();
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		Object lastValue = null;
		TranErrorWrapper tranErrorWrapper = new TranErrorWrapper(importContext);
		int batchsize = importContext.getStoreBatchSize();
		try {

			BatchContext batchContext = new BatchContext();
			while (true) {
				if(!tranErrorWrapper.assertCondition()) {
					jdbcResultSet.stop();
					tranErrorWrapper.throwError();
				}
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){//强制flush操作
					if (count > 0) {
						String datas = builder.toString();
						builder.setLength(0);


						int _count = count;
						count = 0;
						FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
								_count, taskNo, totalCount.getJobNo(),fileUtil,lastValue);
						taskCommand.setDatas(datas);
						tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
						taskNo++;

					}
					continue;
				}
				else if(!hasNext.booleanValue())
					break;

				if(lastValue == null)
					lastValue = importContext.max(currentValue,getLastValue());
				else{
					lastValue = importContext.max(lastValue,getLastValue());
				}
				Context context = importContext.buildContext(jdbcResultSet, batchContext);
//				Context context = new ContextImpl(importContext, jdbcResultSet, batchContext);
				context.refactorData();
				context.afterRefactor();
				if (context.isDrop()) {
					totalCount.increamentIgnoreTotalCount();
					continue;
				}
//				evalBuilk(this.jdbcResultSet,  batchContext,writer, context, versionUpper7);
				CommonRecord record = buildRecord(  context );
				if(builder.length() > 0){
					builder.append(lineSeparator);

				}
				fileFtpOupputContext.generateReocord(context,record, writer);
//					fileUtil.writeData(fileFtpOupputContext.generateReocord(record));
//					//						evalBuilk(this.jdbcResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
				count++;
				if(count >= batchsize ){
					String datas = builder.toString();
					builder.setLength(0);

					int _count = count;
					count = 0;
					FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
							_count, taskNo, totalCount.getJobNo(),fileUtil,lastValue);
					taskCommand.setDatas(datas);
					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
					taskNo++;

				}
			}
			if (count > 0) {
				if(!tranErrorWrapper.assertCondition()) {
					tranErrorWrapper.throwError();
				}
//				if(this.error != null && !importContext.isContinueOnError()) {
//					throw error;
//				}
				String datas = builder.toString();
				FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
						count, taskNo, totalCount.getJobNo(),fileUtil,lastValue);
				taskCommand.setDatas(datas);
				tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
				builder.setLength(0);
				if(isPrintTaskLog())
					logger.info(new StringBuilder().append("submit tasks:").append(taskNo).toString());

			}
			else{
				if(isPrintTaskLog())
					logger.info(new StringBuilder().append("submit tasks:").append(taskNo).toString());
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
			waitTasksComplete(tasks, service, exception, lastValue, totalCount, tranErrorWrapper, new WaitTasksCompleteCallBack() {
				@Override
				public void call() {
					fileUtil.sendFile(fileFtpOupputContext);//传输文件
				}
			});
			try {
				writer.close();
			} catch (Exception e) {

			}
			totalCount.setJobEndTime(new Date());
		}

		return ret;
	}
	/**
	 * 串行批处理导入
	 * @return
	 */
	public String batchExecute(  ){
		int count = 0;
		StringBuilder builder = new StringBuilder();
		BBossStringWriter writer = new BBossStringWriter(builder);
		String ret = null;
		int taskNo = 0;
		Exception exception = null;
		Status currentStatus = importContext.getCurrentStatus();
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		Object lastValue = null;
		long start = System.currentTimeMillis();
		long istart = 0;
		long end = 0;
		long totalCount = 0;
		long ignoreTotalCount = 0;

		ImportCount importCount = new SerialImportCount();
		int batchsize = importContext.getStoreBatchSize();
		try {
			istart = start;
			BatchContext batchContext = new BatchContext();
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					if(count > 0) {
						String _dd =  builder.toString();
						builder.setLength(0);
						int _count = count;
						count = 0;
						FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
								_count, taskNo, importCount.getJobNo(),fileUtil,lastValue);
						taskCommand.setDatas(_dd);
						ret = TaskCall.call(taskCommand);


						if (isPrintTaskLog()) {
							end = System.currentTimeMillis();
							logger.info(new StringBuilder().append("Force flush datas Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
									.append(",import ").append(_count).append(" records.").toString());
							istart = end;
						}
						taskNo ++;
						totalCount += _count;
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
				Context context = importContext.buildContext(jdbcResultSet, batchContext);
//				Context context = new ContextImpl(importContext, jdbcResultSet, batchContext);
				context.refactorData();
				context.afterRefactor();
				if (context.isDrop()) {
					importCount.increamentIgnoreTotalCount();
					continue;
				}
				CommonRecord record = buildRecord(  context );
				if(builder.length() > 0){
					builder.append(lineSeparator);

				}
				fileFtpOupputContext.generateReocord(context,record, writer);
				count++;
				if (count >= batchsize) {
					writer.flush();
					String datas = builder.toString();
					builder.setLength(0);
					writer.close();
					writer = new BBossStringWriter(builder);

					int _count = count;
					count = 0;
					FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
							_count, taskNo, importCount.getJobNo(),fileUtil,lastValue);
					taskCommand.setDatas(datas);
					ret = TaskCall.call(taskCommand);



					if(isPrintTaskLog())  {
						end = System.currentTimeMillis();
						logger.info(new StringBuilder().append("Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
								.append(",import ").append(batchsize).append(" records.").toString());
						istart = end;
					}
					totalCount += count;
					taskNo ++;

				}

				if(fileFtpOupputContext.getMaxFileRecordSize() > 0 && totalCount > 0
						&& (totalCount % fileFtpOupputContext.getMaxFileRecordSize() == 0)){//reached max file record size
					if (count > 0 ) {
						String _dd = builder.toString();
						builder.setLength(0);
						FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext, targetImportContext,
								count, taskNo, importCount.getJobNo(), fileUtil, lastValue);
						taskCommand.setDatas(_dd);
						TaskCall.call(taskCommand);
						totalCount += count;
						taskNo++;
					}
					fileUtil.sendFile(fileFtpOupputContext);
					fileUtil = this.initFileUtil();
				}

			}
			if (count > 0) {
				writer.flush();
				String datas = builder.toString();

				FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext, targetImportContext,
						count, taskNo, importCount.getJobNo(), fileUtil, lastValue);
				taskCommand.setDatas(datas);
				TaskCall.call(taskCommand);
				if(isPrintTaskLog())  {
					end = System.currentTimeMillis();
					logger.info(new StringBuilder().append("Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
							.append(",import ").append(count).append(" records,IgnoreTotalCount ")
							.append(ignoreTotalCount).append(" records.").toString());

				}
				totalCount += count;
				fileUtil.sendFile(fileFtpOupputContext);
			}
			if(isPrintTaskLog()) {
				end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Execute Tasks:").append(taskNo).append(",All Take time:").append((end - start)).append("ms")
						.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
						.append(ignoreTotalCount).append(" records.").toString());

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

			if(!TranErrorWrapper.assertCondition(exception ,importContext)){
				stop();
			}
			try {
				writer.close();
			} catch (Exception e) {

			}
			importCount.setJobEndTime(new Date());
		}

		return ret;
	}


	private void appendFieldValues(CommonRecord record,
								   String[] columns ,
			List<FieldMeta> fieldValueMetas,
			Map<String, Object> addedFields) {
		if(fieldValueMetas ==  null || fieldValueMetas.size() == 0){
			return;
		}
		int i = 0;
		Param param = null;
		for(String name:columns){
			if(addedFields.containsKey(name))
				continue;
			for(FieldMeta fieldMeta:fieldValueMetas){
				if(name.equals(fieldMeta.getEsFieldName())){
					record.addData(name,fieldMeta.getValue());
					addedFields.put(name,dummy);
					break;
				}
			}
		}
	}




}
