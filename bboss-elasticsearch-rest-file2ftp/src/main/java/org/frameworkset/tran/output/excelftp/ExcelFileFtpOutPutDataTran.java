package org.frameworkset.tran.output.excelftp;

import com.frameworkset.orm.annotation.BatchContext;
import org.frameworkset.elasticsearch.ElasticSearchException;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.ParallImportCount;
import org.frameworkset.tran.metrics.SerialImportCount;
import org.frameworkset.tran.output.fileftp.FileFtpOutPutDataTran;
import org.frameworkset.tran.output.fileftp.FileOupputContext;
import org.frameworkset.tran.output.fileftp.FileTransfer;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCall;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ExcelFileFtpOutPutDataTran extends FileFtpOutPutDataTran {


	protected FileTransfer buildFileTransfer(FileOupputContext fileOupputContext, String fileName) throws IOException {
		FileTransfer fileTransfer = new ExcelFileTransfer(taskInfo, fileOupputContext,path,fileName);

		return fileTransfer;

	}

	public ExcelFileFtpOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext, targetImportContext,  currentStatus);
	}
	public ExcelFileFtpOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, CountDownLatch countDownLatch, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext, targetImportContext,countDownLatch,  currentStatus);
	}


	@Override
	public String serialExecute(){
		logger.info("serial import data Execute started.");
		List<CommonRecord> records = new ArrayList<>();
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

//			List<DBRecord> records = new ArrayList<DBRecord>();
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					String ret = null;
					if(records.size() > 0) {

						ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
								totalCount, taskNo, importCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
						taskCommand.setDatas(records);
						ret = TaskCall.call(taskCommand);
						taskNo ++;
						records = new ArrayList<>();
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
					CommonRecord record = buildRecord(  context );
					records.add(record);

//					fileUtil.writeData(fileFtpOupputContext.generateReocord(record));
//					//						evalBuilk(this.jdbcResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
					totalCount++;
					if(fileOupputContext.getMaxFileRecordSize() > 0 && totalCount > 0
							&& (totalCount % fileOupputContext.getMaxFileRecordSize() == 0)){//reached max file record size

						ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
								totalCount, taskNo, importCount.getJobNo(), (ExcelFileTransfer)fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
						taskCommand.setDatas(records);
						TaskCall.call(taskCommand);
						taskNo ++;
						records = new ArrayList<>();
						fileTransfer.sendFile();
						fileTransfer = this.initFileTransfer();
					}
				} catch (Exception e) {
					throw new DataImportException(e);
				}
			}
			if(records.size() > 0) {

				ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
						totalCount, taskNo, importCount.getJobNo(), (ExcelFileTransfer)fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
				taskNo ++;
				taskCommand.setDatas(records);
				TaskCall.call(taskCommand);

//				importContext.flushLastValue(lastValue);
				fileTransfer.sendFile();//传输文件
			}
			else{
				if(!fileTransfer.isSended()){
					fileTransfer.sendFile();
				}
			}
			if(isPrintTaskLog()) {
				long end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Serial import Take time:").append((end - start)).append("ms")
						.append(",Total Import  ").append(totalCount).append(" records,Total Ignore Count ")
						.append(importCount.getIgnoreTotalCount()).append(" records,Total Failed Count ")
						.append(importCount.getFailedCount()).append(" records.").toString());

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

	/**
	 * 并行批处理导入，ftp上传，不支持并行生成文件

	 * @return
	 */
	@Override
	public String parallelBatchExecute( ){
		if(!fileOupputContext.disableftp()){
			return batchExecute();
		}
		int count = 0;
		List<CommonRecord> records = new ArrayList<>();
		String ret = null;
		ExecutorService	service = importContext.buildThreadPool();
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

			BatchContext batchContext = new BatchContext();
			while (true) {
				if(!tranErrorWrapper.assertCondition()) {
					jdbcResultSet.stop();
					tranErrorWrapper.throwError();
				}
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){//强制flush操作
					if (count > 0) {



						int _count = count;
						count = 0;
						ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
								_count, taskNo, totalCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
						taskCommand.setDatas(records);
						records = new ArrayList<>();
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
				Context context = importContext.buildContext(taskContext,jdbcResultSet, batchContext);
				if(!reachEOFClosed)
					reachEOFClosed = context.reachEOFClosed();
//				Context context = new ContextImpl(importContext, jdbcResultSet, batchContext);
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
				CommonRecord record = buildRecord(  context );

				records.add(record);
				count++;
				if(count >= batchsize ){

					int _count = count;
					count = 0;
					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
							_count, taskNo, totalCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
					taskCommand.setDatas(records);
					records = new ArrayList<>();
					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
					taskNo++;

				}
			}
			if (count > 0) {
				if(!tranErrorWrapper.assertCondition()) {
					tranErrorWrapper.throwError();
				}
				ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
						count, taskNo, totalCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
				taskCommand.setDatas(records);
				tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
				if(isPrintTaskLog())
					logger.info(new StringBuilder().append("Pararrel batch submit tasks:").append(taskNo).toString());

			}
			else{
				if(isPrintTaskLog())
					logger.info(new StringBuilder().append("Pararrel batch submit tasks:").append(taskNo).toString());
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
					fileTransfer.sendFile();//传输文件
				}
			},reachEOFClosed);

			totalCount.setJobEndTime(new Date());
		}

		return ret;
	}
	/**
	 * 串行批处理导入
	 * @return
	 */
	@Override
	public String batchExecute(  ){
		int count = 0;
		List<CommonRecord> records = new ArrayList<>();
		String ret = null;
		int taskNo = 0;
		Exception exception = null;
//		Status currentStatus = importContext.getCurrentStatus();
		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		Object lastValue = null;
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
			BatchContext batchContext = new BatchContext();
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					if(count > 0) {

						int _count = count;
						count = 0;
						ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
								_count, taskNo, importCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
						taskCommand.setDatas(records);
						records = new ArrayList<>();
						ret = TaskCall.call(taskCommand);


						if (isPrintTaskLog()) {
							end = System.currentTimeMillis();
							logger.info(new StringBuilder().append("Batch import Force flush datas Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
									.append(",import ").append(_count).append(" records.").toString());
							istart = end;
						}
						taskNo ++;

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
				Context context = importContext.buildContext(taskContext,jdbcResultSet, batchContext);

				if(!reachEOFClosed)
					reachEOFClosed = context.reachEOFClosed();
//				Context context = new ContextImpl(importContext, jdbcResultSet, batchContext);
				if(context.removed()){
					if(!reachEOFClosed) {//如果是文件末尾，那么是空行记录，不需要记录忽略信息，
						importCount.increamentIgnoreTotalCount();
					}
					else{
						importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
					}

					continue;
				}
				context.refactorData();
				context.afterRefactor();
				if (context.isDrop()) {
					importCount.increamentIgnoreTotalCount();
					continue;
				}
				CommonRecord record = buildRecord(  context );

				records.add(record);
				count++;
				totalCount ++;
				if (count >= batchsize) {


					int _count = count;
					count = 0;
					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
							_count, taskNo, importCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
					taskCommand.setDatas(records);
					records = new ArrayList<>();
					ret = TaskCall.call(taskCommand);



					if(isPrintTaskLog())  {
						end = System.currentTimeMillis();
						logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
								.append(",import ").append(batchsize).append(" records.").toString());
						istart = end;
					}
					taskNo ++;

				}

				if(fileOupputContext.getMaxFileRecordSize() > 0 && totalCount > 0
						&& (totalCount % fileOupputContext.getMaxFileRecordSize() == 0)){//reached max file record size
					if (count > 0 ) {

						ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext, targetImportContext,
								count, taskNo, importCount.getJobNo(), (ExcelFileTransfer) fileTransfer, lastValue,  currentStatus,reachEOFClosed,taskContext);
						taskCommand.setDatas(records);
						records = new ArrayList<>();
						TaskCall.call(taskCommand);
						count = 0;
						taskNo++;
					}
					fileTransfer.sendFile();
					fileTransfer = this.initFileTransfer();
				}

			}
			if (count > 0) {

				ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext, targetImportContext,
						count, taskNo, importCount.getJobNo(), (ExcelFileTransfer) fileTransfer, lastValue,  currentStatus,reachEOFClosed,taskContext);
				taskCommand.setDatas(records);
				TaskCall.call(taskCommand);
				if(isPrintTaskLog())  {
					end = System.currentTimeMillis();
					logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
							.append(",import ").append(count).append(" records,IgnoreTotalCount ")
							.append(ignoreTotalCount).append(" records.").toString());

				}
				fileTransfer.sendFile();
			}
			else{
				if(!fileTransfer.isSended()){
					fileTransfer.sendFile();
				}
			}
			if(isPrintTaskLog()) {
				end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Batch import Execute Tasks:").append(taskNo).append(",All Take time:").append((end - start)).append("ms")
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





}
