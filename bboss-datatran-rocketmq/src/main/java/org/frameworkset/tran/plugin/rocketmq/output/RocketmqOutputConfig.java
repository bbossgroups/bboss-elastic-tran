package org.frameworkset.tran.plugin.rocketmq.output;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.metrics.TimeWindowExportResultHandler;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.*;

import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/23 11:42
 * @author biaoping.yin
 * @version 1.0
 */
public class RocketmqOutputConfig extends BaseConfig implements OutputConfig {
	private Map<String,Object> productConfigs = null;
	public static final String metricKey_rocketmqoutputplugin = "rocketmqoutputplugin";
	/**
	 * 是否启用success监控指标按照聚合统计后的指标输出，默认false
	 * true 启用
	 * false 不启用
	 */
	private boolean enableMetricsAgg;
	/**
	 * 聚合统计时间窗口，默认1分钟
	 */
	private int metricsAggWindow = 60;

    private boolean rocketmqAsynSend = true;

    @Deprecated
    private RecordGenerator recordGenerator;

    private RecordGeneratorV1 recordGeneratorV1;
    private String topic;
    
//	private long logsendTaskMetric = 10000l;



    private String tag;
    private String productGroup;
    private String namesrvAddr;
    private String valueCodecSerial;
    private String keyCodecSerial;

    private String accessKey;
    private String secretKey;
    private String securityToken;
    private String signature;


    private Boolean enableSsl ;
    
	public boolean isRocketmqAsynSend() {
		return rocketmqAsynSend;
	}


	public RocketmqOutputConfig setRocketmqAsynSend(boolean rocketmqAsynSend) {
		this.rocketmqAsynSend = rocketmqAsynSend;
		return this;
	}

 
    @Deprecated
    /**
     * use  public void setRecordGeneratorV1(RecordGeneratorV1 recordGeneratorV1)
     */
	public RocketmqOutputConfig setRecordGenerator(RecordGenerator recordGenerator) {
		this.recordGenerator = recordGenerator;
		return this;
	}
	public void generateReocord(TaskContext taskContext, TaskMetrics taskMetrics, CommonRecord record, Writer builder) throws Exception{
		if(builder == null){
			builder = RecordGeneratorV1.tranDummyWriter;
		}
        RecordGeneratorContext recordGeneratorContext = new RecordGeneratorContext();
        recordGeneratorContext.setRecord(record);
        recordGeneratorContext.setTaskContext(taskContext);
        recordGeneratorContext.setBuilder(builder);
        recordGeneratorContext.setTaskMetrics(taskMetrics).setMetricsLogAPI(taskContext.getDataTranPlugin());

        getRecordGeneratorV1().buildRecord(  recordGeneratorContext);
//		getRecordGenerator().buildRecord(context,taskMetrics,record,builder);
	}
	/**
	 * 输出文件记录处理器:org.frameworkset.tran.util.ReocordGenerator
	 */

	public String getTopic() {
		return topic;
	}

	public RocketmqOutputConfig setTopic(String topic) {
		this.topic = topic;
		return this;
	}

	
	public RocketmqOutputConfig addRocketmqProperty(String name, Object value){
		if(productConfigs == null)
            productConfigs = new LinkedHashMap<>();
        productConfigs.put(name,value);
		return this;
	}
	public RocketmqOutputConfig addRocketmqProperties(Map<String,Object> properties){
		if(properties == null || properties.size() == 0)
			return this;
		if(productConfigs == null)
            productConfigs = new LinkedHashMap<>();
        productConfigs.putAll(properties);
		return this;
	}

    public Map<String, Object> getProductConfigs() {
        return productConfigs;
    }
    //	public long getLogsendTaskMetric() {
//		return logsendTaskMetric;
//	}
//
//	public KafkaOutputConfig setLogsendTaskMetric(long logsendTaskMetric) {
//		this.logsendTaskMetric = logsendTaskMetric;
//		return this;
//	}

	@Override
	public void build(ImportContext importContext, ImportBuilder importBuilder) {
		if(recordGenerator == null && recordGeneratorV1 == null){
			recordGeneratorV1 = new JsonRecordGenerator();
		}
        if(recordGeneratorV1 == null){
            recordGeneratorV1 = new DefaultRecordGeneratorV1(recordGenerator);
        }
	}

	public WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		if(enableMetricsAgg) {
			TimeWindowExportResultHandler timeWindowExportResultHandler = new TimeWindowExportResultHandler(metricKey_rocketmqoutputplugin,
					exportResultHandler, this);
			return timeWindowExportResultHandler;
		}
		else{
			return super.buildExportResultHandler(exportResultHandler);
		}
	}


	public boolean isEnableMetricsAgg() {
		return enableMetricsAgg;
	}

	public RocketmqOutputConfig setEnableMetricsAgg(boolean enableMetricsAgg) {
		this.enableMetricsAgg = enableMetricsAgg;
		return this;
	}

	@Override
	public int getMetricsAggWindow() {
		return metricsAggWindow;
	}

	public RocketmqOutputConfig setMetricsAggWindow(int metricsAggWindow) {
		this.metricsAggWindow = metricsAggWindow;
		return this;
	}

    public RecordGeneratorV1 getRecordGeneratorV1() {
        return recordGeneratorV1;
    }

    public RocketmqOutputConfig setRecordGeneratorV1(RecordGeneratorV1 recordGeneratorV1) {
        this.recordGeneratorV1 = recordGeneratorV1;
        return this;
    }


    @Override
    public OutputPlugin getOutputPlugin(ImportContext importContext) {
        return new RocketmqOutputDataTranPlugin(importContext);
    }


    public String getProductGroup() {
        return productGroup;
    }

    public RocketmqOutputConfig setProductGroup(String productGroup) {
        this.productGroup = productGroup;
        return this;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public RocketmqOutputConfig setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
        return this;
    }

    public String getValueCodecSerial() {
        return valueCodecSerial;
    }

    public RocketmqOutputConfig setValueCodecSerial(String valueCodecSerial) {
        this.valueCodecSerial = valueCodecSerial;
        return this;
    }

    public String getKeyCodecSerial() {
        return keyCodecSerial;
    }

    public RocketmqOutputConfig setKeyCodecSerial(String keyCodecSerial) {
        this.keyCodecSerial = keyCodecSerial;
        return this;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public RocketmqOutputConfig setAccessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public RocketmqOutputConfig setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public String getSecurityToken() {
        return securityToken;
    }

    public RocketmqOutputConfig setSecurityToken(String securityToken) {
        this.securityToken = securityToken;
        return this;
    }

    public String getSignature() {
        return signature;
    }

    public RocketmqOutputConfig setSignature(String signature) {
        this.signature = signature;
        return this;
    }

    public Boolean getEnableSsl() {
        return enableSsl;
    }

    public RocketmqOutputConfig setEnableSsl(Boolean enableSsl) {
        this.enableSsl = enableSsl;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

}
