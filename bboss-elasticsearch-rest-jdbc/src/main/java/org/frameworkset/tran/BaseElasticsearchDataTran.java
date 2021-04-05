package org.frameworkset.tran;

import com.frameworkset.common.poolman.handle.ValueExchange;
import com.frameworkset.orm.annotation.BatchContext;
import com.frameworkset.orm.annotation.ESIndexWrapper;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.ElasticSearchException;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.BuildTool;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.serial.CharEscapeUtil;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.elasticsearch.template.ESUtil;
import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.JDBCGetVariableValue;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommandImpl;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.ParallImportCount;
import org.frameworkset.tran.metrics.SerialImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.task.TaskCall;
import org.frameworkset.util.annotations.DateFormateMeta;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class BaseElasticsearchDataTran extends BaseDataTran{
	private ClientInterface[] clientInterfaces;
	private boolean versionUpper7;;
	protected String taskInfo;
	private String elasticsearch;

	@Override
	public void logTaskStart(Logger logger) {

		logger.info(taskInfo);
	}

	private void initClientInterfaces(String elasticsearchs){
		if(elasticsearchs != null) {
			String[] _elasticsearchs = elasticsearchs.split(",");
			clientInterfaces = new ClientInterface[_elasticsearchs.length];
			for(int i = 0; i < _elasticsearchs.length; i ++) {
				clientInterfaces[i] = ElasticSearchHelper.getRestClientUtil(_elasticsearchs[i]);
			}

		}
		else{
			clientInterfaces = new ClientInterface[1];
			clientInterfaces[0] = ElasticSearchHelper.getRestClientUtil("default");
		}
		if(clientInterfaces != null && clientInterfaces.length > 0)
			versionUpper7 = clientInterfaces[0].isVersionUpper7();
	}
	public BaseElasticsearchDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext,Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext,targetImportContext,  currentStatus);

		String elasticsearch =  targetImportContext.getTargetElasticsearch();
		if(elasticsearch == null)
			elasticsearch = "default";
		this.elasticsearch = elasticsearch;

//		clientInterface = ElasticSearchHelper.getRestClientUtil(elasticsearch);
	}

	@Override
	public void init() {
		super.init();
		initClientInterfaces(elasticsearch);
		if(targetImportContext.getEsIndexWrapper() == null){
			throw new ESDataImportException("Global Elasticsearch index must be setted, please check your import job builder config.");
		}
		taskInfo = new StringBuilder().append("import data to elasticsearch[").append(elasticsearch).append("] ")
				.append(" IndexName[").append(targetImportContext.getEsIndexWrapper().getIndex())
				.append("] IndexType[").append(targetImportContext.getEsIndexWrapper().getType())
				.append("] start.").toString();
	}

	public BaseElasticsearchDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, String esCluster,Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,   targetImportContext,  currentStatus);
		this.elasticsearch = esCluster;
//		clientInterface = ElasticSearchHelper.getRestClientUtil(esCluster);
	}

//	public BaseDataTran(String esCluster) {
//		clientInterface = ElasticSearchHelper.getRestClientUtil(esCluster);
//	}



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
						writer.flush();
						String datas = builder.toString();
						builder.setLength(0);
						writer.close();
						writer = new BBossStringWriter(builder);

						int _count = count;
						count = 0;
						for(ClientInterface clientInterface:clientInterfaces) {
							taskNo++;
							TaskCommandImpl taskCommand = new TaskCommandImpl(totalCount, importContext,targetImportContext,
									_count, taskNo, totalCount.getJobNo(),lastValue,  currentStatus,reachEOFClosed);
//						count = 0;
							taskCommand.setClientInterface(clientInterface);
							taskCommand.setDatas(datas);
							tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
						}
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
					totalCount.increamentIgnoreTotalCount();
					continue;
				}
				context.refactorData();
				context.afterRefactor();
				if (context.isDrop()) {
					totalCount.increamentIgnoreTotalCount();
					continue;
				}
				evalBuilk(this.jdbcResultSet,  batchContext,writer, context, versionUpper7);
				count++;
				if (count >= batchsize) {
					writer.flush();
					String datas = builder.toString();
					builder.setLength(0);
					writer.close();
					writer = new BBossStringWriter(builder);
					int _count = count;
					count = 0;
					for(ClientInterface clientInterface:clientInterfaces) {
						taskNo++;
						TaskCommandImpl taskCommand = new TaskCommandImpl(totalCount, importContext,targetImportContext, _count,
								taskNo, totalCount.getJobNo(),lastValue,  currentStatus,reachEOFClosed);

						taskCommand.setClientInterface(clientInterface);
						taskCommand.setDatas(datas);
						tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
					}
				}

			}
			if (count > 0) {
				if(!tranErrorWrapper.assertCondition()) {
					tranErrorWrapper.throwError();
				}
//				if(this.error != null && !importContext.isContinueOnError()) {
//					throw error;
//				}
				writer.flush();
				String datas = builder.toString();
				for(ClientInterface clientInterface:clientInterfaces) {
					taskNo++;
					TaskCommandImpl taskCommand = new TaskCommandImpl(totalCount, importContext,targetImportContext,
							count, taskNo, totalCount.getJobNo(),lastValue,  currentStatus,reachEOFClosed);
					taskCommand.setClientInterface(clientInterface);
					taskCommand.setDatas(datas);
					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));

				}
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
						writer.flush();
						String datas = builder.toString();
						builder.setLength(0);
						writer.close();
						writer = new BBossStringWriter(builder);

						int _count = count;
						count = 0;
						for(ClientInterface clientInterface:clientInterfaces) {
							taskNo++;
							TaskCommandImpl taskCommand = new TaskCommandImpl(importCount, importContext,targetImportContext,
									_count, taskNo, importCount.getJobNo(),lastValue,  currentStatus,reachEOFClosed);
//						int temp = count;
//						count = 0;

							taskCommand.setClientInterface(clientInterface);
							taskCommand.setDatas(datas);
							ret = TaskCall.call(taskCommand);
						}
//						importContext.flushLastValue(lastValue);


						if (isPrintTaskLog()) {
							end = System.currentTimeMillis();
							logger.info(new StringBuilder().append("Batch import Force flush datas Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
									.append(",import ").append(_count).append(" records.").toString());
							istart = end;
						}
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
				Context context = importContext.buildContext(taskContext,jdbcResultSet, batchContext);
//				Context context = new ContextImpl(importContext, jdbcResultSet, batchContext);
				if(!reachEOFClosed)
					reachEOFClosed = context.reachEOFClosed();
				if(context.removed()){
					importCount.increamentIgnoreTotalCount();
					continue;
				}
				context.refactorData();
				context.afterRefactor();
				if (context.isDrop()) {
					importCount.increamentIgnoreTotalCount();
					continue;
				}
				evalBuilk(  this.jdbcResultSet,batchContext,writer,   context, versionUpper7);
				count++;
				if (count >= batchsize) {
					writer.flush();
					String datas = builder.toString();
					builder.setLength(0);
					writer.close();
					writer = new BBossStringWriter(builder);

					int _count = count;
					count = 0;
					for(ClientInterface clientInterface:clientInterfaces) {
						taskNo++;
						TaskCommandImpl taskCommand = new TaskCommandImpl(importCount, importContext,targetImportContext,
								_count, taskNo, importCount.getJobNo(),lastValue,  currentStatus,reachEOFClosed);
//					count = 0;
						taskCommand.setClientInterface(clientInterface);
						taskCommand.setDatas(datas);
						ret = TaskCall.call(taskCommand);
					}
//					importContext.flushLastValue(lastValue);


					if(isPrintTaskLog())  {
						end = System.currentTimeMillis();
						logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
								.append(",import ").append(batchsize).append(" records.").toString());
						istart = end;
					}
					totalCount += count;


				}

			}
			if (count > 0) {
				writer.flush();
				String datas = builder.toString();
				for(ClientInterface clientInterface:clientInterfaces) {
					taskNo++;
					TaskCommandImpl taskCommand = new TaskCommandImpl(importCount, importContext,targetImportContext,
							count, taskNo, importCount.getJobNo(),lastValue,  currentStatus,reachEOFClosed);
					taskCommand.setClientInterface(clientInterface);
					taskCommand.setDatas(datas);
					ret = TaskCall.call(taskCommand);
				}
//				importContext.flushLastValue(lastValue);
				if(isPrintTaskLog())  {
					end = System.currentTimeMillis();
					logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
							.append(",import ").append(count).append(" records,IgnoreTotalCount ")
							.append(ignoreTotalCount).append(" records.").toString());

				}
				totalCount += count;
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

	public String serialExecute(  ){
		StringBuilder builder = new StringBuilder();
		BBossStringWriter writer = new BBossStringWriter(builder);
		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
//		Status currentStatus = importContext.getCurrentStatus();
		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		long totalCount = 0;
		ImportCount importCount = new SerialImportCount();
		long ignoreTotalCount = 0;
		boolean reachEOFClosed = false;
		try {
			BatchContext batchContext =  new BatchContext();
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){ //强制flush数据
					writer.flush();
					String ret = null;
					if(builder.length() > 0) {
						String _dd =  builder.toString();
						builder.setLength(0);
						for(ClientInterface clientInterface:clientInterfaces) {

							TaskCommandImpl taskCommand = new TaskCommandImpl(importCount, importContext,targetImportContext,
									totalCount, 1, importCount.getJobNo(),lastValue,  currentStatus,reachEOFClosed);
							taskCommand.setClientInterface(clientInterface);
							taskCommand.setDatas(_dd);
							ret = TaskCall.call(taskCommand);
						}
					}
					else{
						ret = "{\"took\":0,\"errors\":false}";
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
					if(lastValue == null)
						lastValue = importContext.max(currentValue,getLastValue());
					else{
						lastValue = importContext.max(lastValue,getLastValue());
					}
//					Context context = new ContextImpl(importContext, jdbcResultSet, batchContext);
					Context context = importContext.buildContext(taskContext,jdbcResultSet, batchContext);
					if(!reachEOFClosed)
						reachEOFClosed = context.reachEOFClosed();
					if(context.removed()){
						importCount.increamentIgnoreTotalCount();
						continue;
					}
					context.refactorData();
					context.afterRefactor();
					if (context.isDrop()) {
						importCount.increamentIgnoreTotalCount();
						continue;
					}
					evalBuilk(this.jdbcResultSet,  batchContext,writer,  context,  versionUpper7);
					totalCount ++;
				} catch (Exception e) {
					throw new ElasticSearchException(e);
				}

			}
			writer.flush();
			String ret = null;
			if(builder.length() > 0) {
				String _dd =  builder.toString();
				builder.setLength(0);
				for(ClientInterface clientInterface:clientInterfaces) {

					TaskCommandImpl taskCommand = new TaskCommandImpl(importCount, importContext,targetImportContext,
							totalCount, 1, importCount.getJobNo(),lastValue,  currentStatus,reachEOFClosed);
					taskCommand.setClientInterface(clientInterface);
					taskCommand.setDatas(_dd);
					ret = TaskCall.call(taskCommand);
				}
			}
			else{
				ret = "{\"took\":0,\"errors\":false}";
			}
//			importContext.flushLastValue(lastValue);
			if(isPrintTaskLog()) {

				long end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Serial import All Take time:").append((end - start)).append("ms")
						.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
						.append(ignoreTotalCount).append(" records.").toString());

			}
			return ret;
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
			importCount.setJobEndTime(new Date());
		}
	}
	public String tran(String indexName,String indexType) throws ElasticSearchException{
		ESIndexWrapper esIndexWrapper = new ESIndexWrapper(indexName,indexType);
		targetImportContext.setEsIndexWrapper(esIndexWrapper);
		return tran();
	}




	public static void buildMeta(Context context, Writer writer ,boolean upper7) throws Exception {
		Object id = context.getEsId();
		Object parentId = context.getParentId();
		Object routing = context.getRouting();
		ClientOptions clientOptions = context.getClientOptions();
		Object esRetryOnConflict = clientOptions != null?clientOptions.getEsRetryOnConflict():null;
		ESIndexWrapper esIndexWrapper = context.getESIndexWrapper();

		JDBCGetVariableValue jdbcGetVariableValue = new JDBCGetVariableValue(context);
		writer.write("{ \"");
		writer.write(context.getOperation());
		writer.write("\" : { \"_index\" : \"");

		if (esIndexWrapper == null ) {
			throw new ESDataImportException(" ESIndex not seted." );
		}
		BuildTool.buildIndiceName(esIndexWrapper,writer,jdbcGetVariableValue);

		writer.write("\"");
		if(!upper7) {
			writer.write(", \"_type\" : \"");
			if (esIndexWrapper == null ) {
				throw new ESDataImportException(" ESIndex type not seted." );
			}
			String indexType = BuildTool.buildIndiceType(esIndexWrapper,jdbcGetVariableValue);
			if(indexType == null || indexType.equals("")){
				throw new ESDataImportException(" ESIndex type not seted." );
			}
			writer.write(indexType);
			writer.write("\"");
		}
		if(id != null) {
			writer.write(", \"_id\" : ");
			BuildTool.buildId(id, writer, true);
		}
		if(parentId != null){
			writer.write(", \"parent\" : ");
			BuildTool.buildId(parentId,writer,true);
		}
		if(routing != null){
			if(!upper7) {
				writer.write(", \"_routing\" : ");
			}
			else{
				writer.write(", \"routing\" : ");
			}
			BuildTool.buildId(routing,writer,true);
		}

//			if(action.equals("update"))
//			{
		if (esRetryOnConflict != null) {
			if(!upper7) {
				writer.write(",\"_retry_on_conflict\":");
			}
			else{
				writer.write(",\"retry_on_conflict\":");
			}
			writer.write(String.valueOf(esRetryOnConflict));
		}
		Object version = context.getVersion();

		if (version != null) {

			writer.write(",\"_version\":");

			writer.write(String.valueOf(version));

		}
		Object versionType = clientOptions!= null?clientOptions.getVersionType():null;
		if(versionType != null) {
			writer.write(",\"_version_type\":\"");
			writer.write(String.valueOf(versionType));
			writer.write("\"");
		}
		/**
		String refresh = clientOptions!= null?clientOptions.getRefresh():null;

		if (refresh != null) {

			writer.write(",\"refresh\":\"");

			writer.write(refresh);
			writer.write("\"");
		}*/
		/**
		String timeout = clientOptions!= null?clientOptions.getTimeout():null;

		if (timeout != null) {

			writer.write(",\"timeout\":\"");

			writer.write(timeout);
			writer.write("\"");
		}
		 */
		/**
		Integer waitForActiveShards = clientOptions!= null?clientOptions.getWaitForActiveShards():null;

		if (waitForActiveShards != null) {

			writer.write(",\"wait_for_active_shards\":");

			writer.write(String.valueOf(waitForActiveShards));
		}
		 */
		/**
		Long if_seq_no = clientOptions!= null?clientOptions.getIfSeqNo():null;

		if (if_seq_no != null) {

			writer.write(",\"if_seq_no\":");

			writer.write(String.valueOf(if_seq_no));
		}
		Long if_primary_term = clientOptions!= null?clientOptions.getIfPrimaryTerm():null;

		if (if_primary_term != null) {

			writer.write(",\"if_primary_term\":");

			writer.write(String.valueOf(if_primary_term));
		}
		 */
//		if(!context.isUpdate()){
		if(upper7) {
				Long if_seq_no = clientOptions != null ? clientOptions.getIfSeqNo() : null;

				if (if_seq_no != null) {

//					if(!upper7) {
//						writer.write(",\"_if_seq_no\":");
//					}
//					else{
//						writer.write(",\"if_seq_no\":");
//					}

					writer.write(",\"if_seq_no\":");

					writer.write(String.valueOf(if_seq_no));
				}

				Long if_primary_term = clientOptions != null ? clientOptions.getIfPrimaryTerm() : null;

				if (if_primary_term != null) {
//					if (!upper7) {
//						writer.write(",\"_if_primary_term\":");
//					} else {
//						writer.write(",\"if_primary_term\":");
//					}
					writer.write(",\"if_primary_term\":");
					writer.write(String.valueOf(if_primary_term));
				}
			}
//			}
			String pipeline = clientOptions!= null?clientOptions.getPipeline():null;

			if (pipeline != null) {

				writer.write(",\"pipeline\":\"");

				writer.write(pipeline);
				writer.write("\"");
			}
//		}
		if(context.isInsert()){

			String op_type = clientOptions!= null?clientOptions.getOpType():null;

			if (op_type != null) {

				writer.write(",\"op_type\":\"");

				writer.write(op_type);
				writer.write("\"");
			}
		}
		writer.write(" } }\n");

	}

	public  void evalBuilk(TranResultSet jdbcResultSet,BatchContext batchContext, Writer writer, Context context, boolean upper7) throws Exception {
		String action = context.getOperation();


		if(context.isInsert()) {
//				SerialUtil.object2json(param,writer);
			buildMeta( context, writer ,       upper7);
			serialResult(  writer,context);
			writer.write("\n");
		}
		else if(context.isUpdate())
		{
			buildMeta( context, writer ,       upper7);
			writer.write("{\"doc\":");
			serialResult(  writer,context);
			ClientOptions clientOptions = context.getClientOptions();
			Object esDocAsUpsert = clientOptions != null?clientOptions.getDocasupsert():null;
			if(esDocAsUpsert != null){
				writer.write(",\"doc_as_upsert\":");
				writer.write(String.valueOf(esDocAsUpsert));
			}
			Object detect_noop = clientOptions != null?clientOptions.getDetectNoop():null;
			if(detect_noop != null){
				writer.write(",\"detect_noop\":");
				writer.write(detect_noop.toString());
			}
			Object esReturnSource = clientOptions != null?clientOptions.getReturnSource():null;
			if(esReturnSource != null){
				writer.write(",\"_source\":");
				writer.write(String.valueOf(esReturnSource));
			}
			List<String> sourceUpdateExcludes  = clientOptions!= null?clientOptions.getSourceUpdateExcludes():null;

			if (sourceUpdateExcludes != null) {
				/**
				if(!upper7) {
					writer.write(",\"_source_excludes\":");
				}
				else{
					writer.write(",\"source_excludes\":");
				}
				 */
				if(!upper7) {
					writer.write(",\"_source_excludes\":");
					SerialUtil.object2json(sourceUpdateExcludes,writer);
				}

			}
			List<String> sourceUpdateIncludes  = clientOptions!= null?clientOptions.getSourceUpdateIncludes():null;

			if (sourceUpdateIncludes != null) {
				/**
				if(!upper7) {
					writer.write(",\"_source_includes\":");
				}
				else{
					writer.write(",\"source_includes\":");
				}
				 */
				if(!upper7) {
					writer.write(",\"_source_includes\":");
					SerialUtil.object2json(sourceUpdateIncludes,writer);
				}


			}
			writer.write("}\n");
		}
		else if(context.isDelete()){
			evalDeleteBuilk(  writer, context,upper7);
		}
		else{
			buildMeta( context, writer ,       upper7);
			serialResult(  writer,context);
			writer.write("\n");
		}


	}

	public static void evalDeleteBuilk(Writer writer,  Context context,boolean isUpper7)  throws Exception{
		/**
		try {
			ESIndexWrapper esIndexWrapper = context.getESIndexWrapper();

			JDBCGetVariableValue jdbcGetVariableValue = new JDBCGetVariableValue(context);
			Object id = context.getEsId();


			writer.write("{ \"delete\" : { \"_index\" : \"");

			BuildTool.buildIndiceName(esIndexWrapper,writer,jdbcGetVariableValue);

			writer.write("\"");
			if(!isUpper7) {
				writer.write(", \"_type\" : \"");
				if (esIndexWrapper == null ) {
					throw new ESDataImportException(" ESIndex type not seted." );
				}
				BuildTool.buildIndiceType(esIndexWrapper,writer,jdbcGetVariableValue);
				writer.write("\"");
			}
			writer.write(", \"_id\" : ");
			BuildTool.buildId(id,writer,true);
			writer.write(" } }\n");


		} catch (Exception e) {
			throw new ElasticSearchException(e);
		}*/
		buildMeta(context,writer,isUpper7);

	}

	private  void serialResult( Writer writer,  Context context) throws Exception {

		TranMeta metaData = context.getMetaData();
		int counts = metaData != null ?metaData.getColumnCount():0;
		writer.write("{");
		Boolean useJavaName = context.getUseJavaName();
		if(useJavaName == null)
			useJavaName = true;

		Boolean useLowcase = context.getUseLowcase();


		if(useJavaName == null) {
			useJavaName = false;
		}
		if(useLowcase == null)
		{
			useLowcase = false;
		}
		boolean hasSeted = false;

		Map<String,Object> addedFields = new HashMap<String,Object>();

		List<FieldMeta> fieldValueMetas = context.getFieldValues();//context优先级高于，全局配置，全局配置高于字段值
		hasSeted = appendFieldValues(  writer,   context, fieldValueMetas,  hasSeted,addedFields);
		fieldValueMetas = context.getESJDBCFieldValues();
		hasSeted = appendFieldValues(  writer,   context, fieldValueMetas,  hasSeted,addedFields);
		for(int i =0; i < counts; i++)
		{
			String colName = metaData.getColumnLabelByIndex(i);
			if(colName.equals("_id")){
				if(logger.isDebugEnabled()){
					logger.debug("Field [_id] is a metadata field and cannot be added inside a document. Use the index API request parameters.");
				}
				continue;
			}
			int sqlType = metaData.getColumnTypeByIndex(i);
//			if("ROWNUM__".equals(colName))//去掉oracle的行伪列
//				continue;
			String javaName = null;
			FieldMeta fieldMeta = context.getMappingName(colName);
			if(fieldMeta != null) {
				if(fieldMeta.getIgnore() != null && fieldMeta.getIgnore() == true)
					continue;
				javaName = fieldMeta.getEsFieldName();
			}
			else {
				if(useJavaName) {
					javaName = metaData.getColumnJavaNameByIndex(i);
				}
				else{
					javaName =  !useLowcase ?colName:metaData.getColumnLabelLowerByIndex(i);
				}
			}
			if(javaName == null){
				javaName = colName;
			}
			if(addedFields.containsKey(javaName)){
				continue;
			}
			Object value = context.getValue(     i,  colName,sqlType);
			if(value == null && importContext.isIgnoreNullValueField()){
				continue;
			}
			if(hasSeted )
				writer.write(",");
			else
				hasSeted = true;

			writer.write("\"");
			writer.write(javaName);
			writer.write("\":");
//			int colType = metaData.getColumnTypeByIndex(i);

			if(value != null) {
				if (value instanceof String) {
					writer.write("\"");
					CharEscapeUtil charEscapeUtil = new CharEscapeUtil(writer);
					charEscapeUtil.writeString((String) value, true);
					writer.write("\"");
				} else if (value instanceof Date) {
					DateFormat dateFormat = null;
					if(fieldMeta != null){
						DateFormateMeta dateFormateMeta = fieldMeta.getDateFormateMeta();
						if(dateFormateMeta != null){
							dateFormat = dateFormateMeta.toDateFormat();
						}
					}
					if(dateFormat == null)
						dateFormat = context.getDateFormat();
					String dataStr = ESUtil.getDate((Date) value,dateFormat);
					writer.write("\"");
					writer.write(dataStr);
					writer.write("\"");
				}
				else if(value instanceof Clob)
				{
					String dataStr = ValueExchange.getStringFromClob((Clob)value);
					writer.write("\"");
					CharEscapeUtil charEscapeUtil = new CharEscapeUtil(writer);
					charEscapeUtil.writeString(dataStr, true);
					writer.write("\"");

				}
				else if(value instanceof Blob){
					String dataStr = ValueExchange.getStringFromBlob((Blob)value);
					writer.write("\"");
					CharEscapeUtil charEscapeUtil = new CharEscapeUtil(writer);
					charEscapeUtil.writeString(dataStr, true);
					writer.write("\"");
				}
				else {
					SimpleStringUtil.object2json(value,writer);//					writer.write(String.valueOf(value));
				}
			}
			else{
				writer.write("null");
			}

		}

		writer.write("}");
	}
	private  boolean appendFieldValues(Writer writer,Context context,
										  List<FieldMeta> fieldValueMetas,boolean hasSeted,Map<String,Object> addedFields) throws IOException {
		if(fieldValueMetas != null && fieldValueMetas.size() > 0){
			for(int i =0; i < fieldValueMetas.size(); i++)
			{

				FieldMeta fieldMeta = fieldValueMetas.get(i);
				String javaName = fieldMeta.getEsFieldName();
				if(addedFields.containsKey(javaName)) {
					if(logger.isInfoEnabled()){
						logger.info(new StringBuilder().append("Ignore adding duplicate field[")
								.append(javaName).append("] value[")
								.append(fieldMeta.getValue())
								.append("].").toString());
					}
					continue;
				}
				Object value = fieldMeta.getValue();
//				if(value == null)
//					continue;
				if(hasSeted)
					writer.write(",");
				else{
					hasSeted = true;
				}

				writer.write("\"");
				writer.write(javaName);
				writer.write("\":");

				if(value != null) {
					if (value instanceof String) {
						writer.write("\"");
						CharEscapeUtil charEscapeUtil = new CharEscapeUtil(writer);
						charEscapeUtil.writeString((String) value, true);
						writer.write("\"");
					} else if (value instanceof Date) {
						DateFormat dateFormat = null;
						if(fieldMeta != null){
							DateFormateMeta dateFormateMeta = fieldMeta.getDateFormateMeta();
							if(dateFormateMeta != null){
								dateFormat = dateFormateMeta.toDateFormat();
							}
						}
						if(dateFormat == null)
							dateFormat = context.getDateFormat();
						String dataStr = ESUtil.getDate((Date) value,dateFormat);
						writer.write("\"");
						writer.write(dataStr);
						writer.write("\"");
					}
					else if(isBasePrimaryType(value.getClass())){
						writer.write(String.valueOf(value));
					}
					else {
						SimpleStringUtil.object2json(value,writer);
					}
				}
				else{
					writer.write("null");
				}
				addedFields.put(javaName,dummy);

			}
		}
		return hasSeted;
	}
	public static final Class[] basePrimaryTypes = new Class[]{Integer.TYPE, Long.TYPE,
								Boolean.TYPE, Float.TYPE, Short.TYPE, Double.TYPE,
								Character.TYPE, Byte.TYPE, BigInteger.class, BigDecimal.class};

	public static boolean isBasePrimaryType(Class type) {
		if (!type.isArray()) {
			if (type.isEnum()) {
				return true;
			} else {
				Class[] var1 = basePrimaryTypes;
				int var2 = var1.length;

				for(int var3 = 0; var3 < var2; ++var3) {
					Class primaryType = var1[var3];
					if (primaryType.isAssignableFrom(type)) {
						return true;
					}
				}

				return false;
			}
		} else {
			return false;
		}
	}



}
