package org.frameworkset.tran;

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseCommonRecordDataTran extends BaseDataTran{

	public BaseCommonRecordDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(taskContext, jdbcResultSet, importContext,   currentStatus);
	}

 

	protected List<CommonRecord> convertDatas(Object datas){
		if(datas == null)
			return null;
		List<CommonRecord> records = null;
		if(datas instanceof List){
			records = (List<CommonRecord>)datas;
		}
		else{
			records = new ArrayList<>(1);
			records.add((CommonRecord)datas);
		}
		return records;

	}
	/**
	 * 并行批处理导入，ftp上传，不支持并行生成文件

	 * @return
	 */
	@Override
	public String parallelBatchExecute( ) {
//        if(!importContext.getDataTranPlugin().onlyUseBatchExecute()) {
            if (logger.isDebugEnabled())
                logger.debug("parallel batch import data Execute started.");

            return tranJob.parallelBatchExecute(parrelTranCommand, currentStatus, importContext, tranResultSet, this);
//        }
//        else{
//            if(logger.isDebugEnabled())
//                logger.debug("batch import data Execute started.");
//            return tranJob.batchExecute(serialTranCommand,currentStatus,importContext, tranResultSet,this);
//        }
	}

	/**
	 * 串行批处理导入

	 * @return
	 */
	@Override
	public String batchExecute(  ){

		if(logger.isDebugEnabled())
			logger.debug("batch import data Execute started.");
		return tranJob.batchExecute(serialTranCommand,currentStatus,importContext, tranResultSet,this);
	}
	/**
	 * 串行处理导入

	 * @return
	 */
	@Override
	public String serialExecute(){
//        if(!importContext.getDataTranPlugin().onlyUseBatchExecute()) {
            if (logger.isDebugEnabled())
                logger.debug("serial import data Execute started.");
            return tranJob.serialExecute(serialTranCommand, currentStatus, importContext, tranResultSet, this);
//        }
//        else{
//            if(logger.isDebugEnabled())
//                logger.debug("batch import data Execute started.");
//            return tranJob.batchExecute(serialTranCommand,currentStatus,importContext, tranResultSet,this);
//        }
	}
 



}
