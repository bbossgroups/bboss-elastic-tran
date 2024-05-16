package org.frameworkset.tran.plugin.http.output;
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
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.http.BaseHttpConfig;
import org.frameworkset.tran.plugin.http.DynamicHeader;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.JsonRecordGenerator;
import org.frameworkset.tran.util.RecordGenerator;
import org.frameworkset.tran.util.TranUtil;

import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/2
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpOutputConfig extends BaseHttpConfig implements OutputConfig {

	private Map<String,Object> httpConfigs;

	private String dataDslName;
	private String dataDsl;
	private String dataKey;
	private boolean json = true;
	public HttpOutputConfig setDslFile(String dslFile){
		this.dslFile = dslFile;
		return this;
	}
	public String getLineSeparator() {
		return lineSeparator;
	}

	public HttpOutputConfig setShowDsl(boolean showDsl){
		this.showDsl = showDsl;
		return this;
	}
	public HttpOutputConfig setJson(boolean json) {
		this.json = json;
		return this;
	}

	public boolean isJson() {
		return json;
	}
	public HttpOutputConfig setDslNamespace(String dslNamespace) {
		this.dslNamespace = dslNamespace;
		return this;
	}
	public HttpOutputConfig setLineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;
		return this;
	}

	private String lineSeparator;

	public String getTargetHttpPool() {
		return targetHttpPool;
	}

	public HttpOutputConfig setTargetHttpPool(String targetHttpPool) {
		this.targetHttpPool = targetHttpPool;
		return this;
	}

	private String targetHttpPool;
	private String serviceUrl;

	public RecordGenerator getRecordGenerator() {
		return recordGenerator;
	}

	public HttpOutputConfig setRecordGenerator(RecordGenerator recordGenerator) {
		this.recordGenerator = recordGenerator;
		return this;
	}

	/**
	 * 输出文件记录处理器:org.frameworkset.tran.kafka.output.fileftp.ReocordGenerator
	 */
	private RecordGenerator recordGenerator;

	public Map<String, Object> getHttpConfigs() {
		return httpConfigs;
	}

	private void checkConfigs(){
		if(httpConfigs == null)
			httpConfigs = new LinkedHashMap<>();
	}
	public HttpOutputConfig addTargetHttpPoolName(String nameProperty,String httpPoolName){
		checkConfigs();
		this.httpConfigs.put(nameProperty,httpPoolName);
		this.targetHttpPool = httpPoolName;
		return this;
	}

	public HttpOutputConfig addHttpOutputConfig(String property,Object value){
		checkConfigs();
		this.httpConfigs.put(property,value);
		return this;
	}

	/**
	 * 添加动态生成的头信息，比如有生命周期的认证token信息等
	 * @param header
	 * @param dynamicHeader
	 * @return
	 */
	public HttpOutputConfig addDynamicHeader(String header, DynamicHeader dynamicHeader){
		_addDynamicHeader(header, dynamicHeader);
		return this;
	}
	/**
	 * 是否直接发送数据到服务端，默认为true，如果需要处理数据
	 */
	private boolean directSendData = true;

	public boolean isDirectSendData() {
		return directSendData;
	}

	public HttpOutputConfig setDataKey(String dataKey) {
		this.dataKey = dataKey;
		return this;
	}

	public String getDataKey() {
		return dataKey;
	}

	@Override
	public void build(ImportBuilder importBuilder) {

		if(SimpleStringUtil.isEmpty(this.getServiceUrl())){

			throw new DataImportException("Input http service url is not setted.");
		}
		if(SimpleStringUtil.isEmpty(httpMethod)){
			httpMethod = "post";
		}
		if(!httpMethod.equals("post") && !httpMethod.equals("put")){
			throw new DataImportException("Input httpMethod must be post or put.");
		}
		postMethod = httpMethod.equals("post");
		if(SimpleStringUtil.isEmpty(this.getDataDsl())){
			if(!SimpleStringUtil.isEmpty(getDslFile()) && !SimpleStringUtil.isEmpty(getDataDslName()) ){
				directSendData = false;
			}
		}
		else{
			directSendData = false;
			if(SimpleStringUtil.isEmpty(dataDslName))
				dataDslName = "datatranSendDslName";
			if(SimpleStringUtil.isEmpty(dslNamespace))
				dslNamespace = "datatranSendDslNamespace"+SimpleStringUtil.getUUID();

			if(SimpleStringUtil.isEmpty(dataKey))
				dataKey = "httpDatas";

		}
		if(getRecordGenerator() == null){
			setRecordGenerator(new JsonRecordGenerator());//默认采用json格式输出数据
		}
		if(json) {
			lineSeparator = ",";
		}
		else {
			if (SimpleStringUtil.isEmpty(lineSeparator))
				lineSeparator = TranUtil.lineSeparator;
		}

	}





	public HttpOutputConfig setHttpMethod(String httpMethod) {
		this.httpMethod = httpMethod;
		return this;
	}


	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new HttpOutputDataTranPlugin(importContext);
	}


	public String getServiceUrl() {
		return serviceUrl;
	}

	public HttpOutputConfig setServiceUrl(String serviceUrl) {
		this.serviceUrl = serviceUrl;
		return this;
	}
	public WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		DefualtExportResultHandler<String> db2ESExportResultHandler = new DefualtExportResultHandler<String>(exportResultHandler);
		return db2ESExportResultHandler;
	}

	public void generateReocord(TaskContext taskContext, CommonRecord record, Writer builder) throws Exception{
		if(builder == null){
			builder = RecordGenerator.tranDummyWriter;
		}

		getRecordGenerator().buildRecord(taskContext, record, builder);
	}

	public HttpOutputConfig addHttpHeaders(Map<String, String> _httpHeaders){
		_addHttpHeaders(_httpHeaders);
		return this;
	}
	public HttpOutputConfig addHttpHeader(String header,String value){
		_addHttpHeader(header,value);
		return this;
	}

	public String getDataDslName() {
		return dataDslName;
	}

	public HttpOutputConfig setDataDslName(String dataDslName) {
		this.dataDslName = dataDslName;
		return this;
	}

	public String getDataDsl() {
		return dataDsl;
	}

	public HttpOutputConfig setDataDsl(String dataDsl) {
		this.dataDsl = dataDsl;
		return this;
	}
}
