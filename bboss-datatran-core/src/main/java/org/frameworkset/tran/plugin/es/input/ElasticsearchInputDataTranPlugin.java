package org.frameworkset.tran.plugin.es.input;
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

import com.frameworkset.util.SimpleStringUtil;
import com.frameworkset.util.ValueCastUtil;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.entity.ESDatas;
import org.frameworkset.elasticsearch.entity.MetaMap;
import org.frameworkset.elasticsearch.template.*;
import org.frameworkset.tran.AsynBaseTranResultSet;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.es.BaseESPlugin;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.schedule.timer.TimeUtil;
import org.frameworkset.util.NumberUtils;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class ElasticsearchInputDataTranPlugin extends BaseESPlugin implements InputPlugin {
	protected String jobType ;

	public ElasticsearchInputDataTranPlugin(ImportContext importContext) {
		super(importContext);
		elasticsearchInputConfig = (ElasticsearchInputConfig) importContext.getInputConfig();
		this.jobType = "ElasticsearchInputDataTranPlugin";
	}
	protected ElasticsearchInputConfig elasticsearchInputConfig;
	@Override
	public void init(){


	}

	@Override
	public String getJobType() {
		return jobType;
	}


	@Override
	public void beforeInit() {
		this.esConfig = elasticsearchInputConfig.getEsConfig();
		this.applicationPropertiesFile = importContext.getApplicationPropertiesFile();
		this.initES();


	}
	@Override
	public void afterInit(){

	}
	public void initStatusTableId(){
		if(dataTranPlugin.isIncreamentImport()) {
			if( elasticsearchInputConfig.getDslFile() != null && !elasticsearchInputConfig.getDslFile().equals("")
					&& SimpleStringUtil.isNotEmpty(elasticsearchInputConfig.getDslName())) {
				try {
					ClientInterface clientInterface = ElasticSearchHelper.getConfigRestClientUtil(elasticsearchInputConfig.getSourceElasticsearch(), elasticsearchInputConfig.getDslFile());
					ESInfo esInfo = clientInterface.getESInfo(elasticsearchInputConfig.getDslName());
					importContext.setStatusTableId(esInfo.getTemplate().hashCode());
				} catch (Exception e) {
					throw ImportExceptionUtil.buildDataImportException(importContext,e);
				}
			}
			else if(SimpleStringUtil.isNotEmpty(elasticsearchInputConfig.getDsl())){
				importContext.setStatusTableId(elasticsearchInputConfig.getDsl().hashCode());
			}
		}
	}



	protected void commonImportData(TaskContext taskContext, BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler) throws Exception {
		Map params = dataTranPlugin.getJobInputParams(taskContext);

		params.put("size", importContext.getFetchSize());//每页5000条记录
		if(elasticsearchInputConfig.isSliceQuery()){
			params.put("sliceMax",elasticsearchInputConfig.getSliceSize());
		}
		Date date = new Date();
		exportESData(  taskContext,  esExporterScrollHandler,  params,date,date);
	}

	protected String getQueryUrl(TaskContext taskContext,Date lastStartValue,Date lastEndValue){
		if(elasticsearchInputConfig.getQueryUrl() != null){
			return elasticsearchInputConfig.getQueryUrl();
		}
		else if(elasticsearchInputConfig.getQueryUrlFunction() != null){
			return elasticsearchInputConfig.getQueryUrlFunction().queryUrl(  taskContext,  lastStartValue,  lastEndValue);
		}
		throw ImportExceptionUtil.buildDataImportException(importContext,"query url or query url function not setted.");
	}

	protected void exportESData(TaskContext taskContext,BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler,Map params,Date lastStartValue,Date lastEndValue){
		if( SimpleStringUtil.isNotEmpty(elasticsearchInputConfig.getDslFile()) && SimpleStringUtil.isNotEmpty(elasticsearchInputConfig.getDslName()) ){
			ClientInterface clientUtil = ElasticSearchHelper.getConfigRestClientUtil(elasticsearchInputConfig.getSourceElasticsearch(),elasticsearchInputConfig.getDslFile());
			dslScriptByConfig( clientUtil,elasticsearchInputConfig.getDslName(),taskContext,esExporterScrollHandler, params, lastStartValue, lastEndValue);
		}
		else if(SimpleStringUtil.isNotEmpty(elasticsearchInputConfig.getDsl())){
			String dslName = elasticsearchInputConfig.getDslName();
			//创建一个从数据库加载命名空间为"datatranDslNamespace-"+SimpleStringUtil.getUUID()的dsl语句的ClientInterface组件实例
			ClientInterface clientUtil = ElasticSearchHelper.getConfigRestClientUtil(elasticsearchInputConfig.getSourceElasticsearch(),new BaseTemplateContainerImpl(elasticsearchInputConfig.getDslNamespace()) {
				@Override
				protected Map<String, TemplateMeta> loadTemplateMetas(String namespace) {
					try {
						BaseTemplateMeta baseTemplateMeta = new BaseTemplateMeta();
						baseTemplateMeta.setName(dslName);
						baseTemplateMeta.setNamespace(namespace);
						baseTemplateMeta.setDslTemplate(elasticsearchInputConfig.getDsl());
						baseTemplateMeta.setMultiparser(true);
						Map<String,TemplateMeta> templateMetaMap = new LinkedHashMap<>();
						templateMetaMap.put(baseTemplateMeta.getName(),baseTemplateMeta);
						return templateMetaMap;
					} catch (Exception e) {
						throw new DSLParserException(e);
					}
				}

				@Override
				protected long getLastModifyTime(String namespace) {
					// 获取dsl更新时间戳：模拟每次都更新，返回当前时间戳
					// 如果检测到时间戳有变化，框架就将调用loadTemplateMetas方法加载最新的dsl配置
					return -1;
				}
			});
			dslScriptByConfig( clientUtil,dslName,taskContext,esExporterScrollHandler, params, lastStartValue, lastEndValue);
		}
		else{
			throw ImportExceptionUtil.buildDataImportException(importContext,"DslFile or DslName or Dsl Script is not setted by ElasticsearchInputConfig.");
		}

	}

	private void dslScriptByConfig(ClientInterface clientUtil,String dslName,
								   TaskContext taskContext,BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler,
								   Map params,Date lastStartValue,Date lastEndValue){
//采用自定义handler函数处理每个scroll的结果集后，response中只会包含总记录数，不会包含记录集合
		//scroll上下文有效期1分钟；大数据量时可以采用handler函数来处理每次scroll检索的结果，规避数据量大时存在的oom内存溢出风险



		ESDatas<MetaMap> response = null;
		if(!elasticsearchInputConfig.isSliceQuery()) {

			if(importContext.isParallel() && esExporterScrollHandler instanceof ESDirectExporterScrollHandler) {
				response = clientUtil.scrollParallel(getQueryUrl(  taskContext,lastStartValue,lastEndValue),
						dslName, elasticsearchInputConfig.getScrollLiveTime(),
						params, MetaMap.class, esExporterScrollHandler);
			}
			else
			{
				response = clientUtil.scroll(getQueryUrl(  taskContext,lastStartValue,lastEndValue),
						dslName, elasticsearchInputConfig.getScrollLiveTime(),
						params, MetaMap.class, esExporterScrollHandler);
			}
		}
		else{
			response = clientUtil.scrollSliceParallel(getQueryUrl(  taskContext,lastStartValue,lastEndValue), dslName,
					params, elasticsearchInputConfig.getScrollLiveTime(),MetaMap.class, esExporterScrollHandler);
		}
		if(logger.isInfoEnabled()) {
			if(response != null) {
				logger.info("Export compoleted and export total {} records.", response.getTotalSize());
			}
			else{
				logger.info("Export compoleted and export no records or failed.");
			}
		}
	}




	/**
	 * 需要根据增量日期字段格式对date进行格式转换，避免采用默认utc格式检索出错
	 * @param date
	 * @return
	 */
	@Override
	public Object formatLastDateValue(Date date){
		String lastValueDateformat = importContext.getLastValueDateformat();
		if(lastValueDateformat != null && !lastValueDateformat.equals("")){
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(lastValueDateformat);
			return simpleDateFormat.format(date);
		}
		return date;


	}

    public Object formatLastLocalDateTimeValue(LocalDateTime localDateTime){
        return TimeUtil.changeLocalDateTime2String(localDateTime,importContext.getLastValueDateformat());
    }
	protected void increamentImportData(TaskContext taskContext,BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler) throws Exception {
		Map params = dataTranPlugin.getJobInputParams(taskContext);
		params.put("size", importContext.getFetchSize());//每页fetchSize条记录
		if(elasticsearchInputConfig.isSliceQuery()){
			params.put("sliceMax",elasticsearchInputConfig.getSliceSize());
		}
		Object[] lastValues = dataTranPlugin.putLastParamValue(params);
		Object lastValue = lastValues[0];
		if(lastValue instanceof Date) {
			Date lastEndValue = null;
			if(importContext.increamentEndOffset() != null){
//				lastEndValue = (Date)params.get(getLastValueVarName()+"__endTime");
				lastEndValue = (Date)lastValues[1];
			}
			else
				lastEndValue = new Date();
			exportESData(  taskContext,esExporterScrollHandler, params, (Date)lastValue,lastEndValue);
		}
        else if(lastValue instanceof LocalDateTime) {
            Date lastEndValue = null;
            if(importContext.increamentEndOffset() != null){
//				lastEndValue = (Date)params.get(getLastValueVarName()+"__endTime");
                lastEndValue = TimeUtil.convertLocalDatetime((LocalDateTime)lastValues[1]);
            }
            else
                lastEndValue = new Date();
            exportESData(  taskContext,esExporterScrollHandler, params,
                    TimeUtil.convertLocalDatetime((LocalDateTime)lastValue),lastEndValue);
        }
		else{
            if(!importContext.isNumberTypeTimestamp()) {
                Date date = new Date();
                exportESData(taskContext, esExporterScrollHandler, params, date, date);
            }
            else{     //数字类型：值为时间戳
                Date endDate = null;
                if(importContext.increamentEndOffset() == null){
                    endDate = new Date();
                }
                else{
                    endDate = (Date) lastValues[1];
                }
                exportESData(taskContext, esExporterScrollHandler, params, new Date(ValueCastUtil.longValue(lastValues[0],null)), endDate);
            }
		}

	}
//	protected abstract BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, CountDownLatch countDownLatch, Status currentStatus);

	public void doImportData(TaskContext taskContext)  throws DataImportException {


		AsynBaseTranResultSet jdbcResultSet = new ES2TranResultSet(importContext);
		final JobCountDownLatch countDownLatch = new JobCountDownLatch(1);
		final BaseDataTran es2DBDataTran = dataTranPlugin.createBaseDataTran( taskContext,jdbcResultSet,countDownLatch,dataTranPlugin.getCurrentStatus());
		ESExporterScrollHandler<MetaMap> esExporterScrollHandler = new ESExporterScrollHandler<MetaMap>(importContext,
				es2DBDataTran);
		try {
			Thread tranThread = new Thread(new Runnable() {
				@Override
				public void run() {
                    try {
                        es2DBDataTran.tran();

                    }
                    catch (DataImportException dataImportException){
                        logger.error("",dataImportException);
                        dataTranPlugin.throwException(  taskContext,  dataImportException);
                        es2DBDataTran.stop2ndClearResultsetQueue(true);
                    }
                    catch (RuntimeException dataImportException){
                        logger.error("",dataImportException);
                        dataTranPlugin.throwException(  taskContext,  dataImportException);
                        es2DBDataTran.stop2ndClearResultsetQueue(true);
                    }
                    catch (Throwable dataImportException){
                        logger.error("",dataImportException);
                        DataImportException dataImportException_ = ImportExceptionUtil.buildDataImportException(importContext,dataImportException);
                        dataTranPlugin.throwException(  taskContext, dataImportException_);
                        es2DBDataTran.stop2ndClearResultsetQueue(true);
                    }

				}
			},"Elasticsearch-Input-Tran");
			tranThread.start();
			if (!importContext.isIncreamentImport()) {

				commonImportData(  taskContext,esExporterScrollHandler);

			} else {

				increamentImportData(  taskContext,esExporterScrollHandler);

			}
		} catch (DataImportException e) {
            es2DBDataTran.stop2ndClearResultsetQueue(true);
			throw e;
		} catch (Exception e) {
            es2DBDataTran.stop2ndClearResultsetQueue(true);
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}
		finally {
			jdbcResultSet.reachEend();
			try {
				countDownLatch.await();

			} catch (InterruptedException e) {
				if(logger.isErrorEnabled())
					logger.error("",e);
			}
			Throwable exception = countDownLatch.getException();
			if(exception != null){
				if(exception instanceof DataImportException)
					throw (DataImportException)exception;
				else
					throw ImportExceptionUtil.buildDataImportException(importContext,exception);
			}
		}

	}




}
