package org.frameworkset.tran.plugin.rocketmq.input;
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

import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.rocketmq.codec.CodecUtil;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.frameworkset.tran.metrics.job.MetricsConfig.DEFAULT_metricsInterval;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/20 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public class RocketmqInputConfig extends BaseConfig<RocketmqInputConfig> implements InputConfig<RocketmqInputConfig> {
	private Map<String,Object> consumerConfigs = null;
    private String accessKey;
    private String secretKey;
    private String securityToken;
    private String signature;


    /**
     * namesrv地址
     */
    private String namesrvAddr ;

    /**
     * CONSUME_FROM_LAST_OFFSET,
     *
     *     @Deprecated
     *     CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
     *     @Deprecated
     *     CONSUME_FROM_MIN_OFFSET,
     *     @Deprecated
     *     CONSUME_FROM_MAX_OFFSET,
     *     CONSUME_FROM_FIRST_OFFSET,
     *     CONSUME_FROM_TIMESTAMP,
     */
    private String consumeFromWhere;
    /**
     * 单位到秒
     * 20191024171201
     */
    private String consumeTimestamp;
    private Boolean enableSsl ;
    private String tag  ;
//    private Long awaitDuration = 10000l;

    private String consumerGroup;



    protected String  topic;
 
    protected Integer maxPollRecords;
    protected Integer consumeMessageBatchMaxSize;
    protected Integer workThreads =20;


	/**
	 * 默认十分钟执行一次拦截器监控数据afterCall方法
	 */
	private long metricsInterval = DEFAULT_metricsInterval;
 
	public static final String CODEC_TEXT = "text";
	public static final String CODEC_TEXT_SPLIT = "text_split";
	public static final String CODEC_LONG = "long";
	public static final String CODEC_JSON = "json";
	public static final String CODEC_INTEGER = "int";
	public static final String CODEC_BYTE = "byte[]";
	/**
	 * json
	 * text
	 */
	private String valueCodec;

	/**
	 * json
	 * text
	 */
	private String keyCodec;

    private String valueDeserializer;


    private String keyDeserializer;
 
	public RocketmqInputConfig setConsumerConfigs(Map<String,Object> consumerConfigs) {
		this.consumerConfigs = consumerConfigs;
		return this;
	}
	public RocketmqInputConfig addConsumerConfig(String key, Object value){
		if(consumerConfigs == null)
            consumerConfigs = new LinkedHashMap<>();
        consumerConfigs.put(key,value);
		return this;
	} 
	public String getValueCodec() {
		return valueCodec;
	}

	public RocketmqInputConfig setValueCodec(String valueCodec) {
		this.valueCodec = valueCodec;
		return this;
	}

	public String getKeyCodec() {
		return keyCodec;
	}

	public RocketmqInputConfig setKeyCodec(String keyCodec) {
		this.keyCodec = keyCodec;
		return this;
	}
 


    
	private void preHandlerCodec(){
		Map<String,Object> properties = this.consumerConfigs;
		if(valueDeserializer == null){

			if(this.getValueCodec() != null) {
                valueDeserializer = CodecUtil.getDeserializer(getValueCodec());
				if(CODEC_TEXT_SPLIT.equals(getValueCodec())){
					properties.put("value.deserializer.splitChar",getFieldSplit());
					properties.put("value.deserializer.cellMappingList",getCellMappingList());
				}
			}
			else{
                valueDeserializer = CodecUtil.getDeserializer(CODEC_JSON);
			}

		}
		if(keyDeserializer == null ){
			if(this.getKeyCodec() != null) {
                keyDeserializer = CodecUtil.getDeserializer(getKeyCodec());
			}
			else{
                keyDeserializer = CodecUtil.getDeserializer(CODEC_TEXT);
			}

		}

	}

	@Override
	public void build(ImportContext importContext,ImportBuilder importBuilder) {
		preHandlerCodec();
	}
	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext){
		DataTranPlugin dataTranPlugin = new RocketmqDataTranPluginImpl(importContext);
		return dataTranPlugin;
	}
	public long getMetricsInterval() {
		return metricsInterval;
	}

	public RocketmqInputConfig setMetricsInterval(long metricsInterval) {
		this.metricsInterval = metricsInterval;
		return this;
	}

    public InputPlugin getInputPlugin(ImportContext importContext) {
        return new RocketmqInputDatatranPlugin(importContext);
    }

    public String getAccessKey() {
        return accessKey;
    }

    public RocketmqInputConfig setAccessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public RocketmqInputConfig setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public String getSecurityToken() {
        return securityToken;
    }

    public RocketmqInputConfig setSecurityToken(String securityToken) {
        this.securityToken = securityToken;
        return this;
    }

    public String getSignature() {
        return signature;
    }

    public RocketmqInputConfig setSignature(String signature) {
        this.signature = signature;
        return this;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public RocketmqInputConfig setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
        return this;
    }

    public String getConsumeFromWhere() {
        return consumeFromWhere;
    }
    /**
     * CONSUME_FROM_LAST_OFFSET,
     *
     *     @Deprecated
     *     CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
     *     @Deprecated
     *     CONSUME_FROM_MIN_OFFSET,
     *     @Deprecated
     *     CONSUME_FROM_MAX_OFFSET,
     *     CONSUME_FROM_FIRST_OFFSET,
     *     CONSUME_FROM_TIMESTAMP,
      
     * @param consumeFromWhere
     * @return
     */
    public RocketmqInputConfig setConsumeFromWhere(String consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
        return this;
    }

    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }

    public RocketmqInputConfig setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
        return this;
    }

    public Boolean getEnableSsl() {
        return enableSsl;
    }

    public RocketmqInputConfig setEnableSsl(Boolean enableSsl) {
        this.enableSsl = enableSsl;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public RocketmqInputConfig setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public RocketmqInputConfig setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, Object> getConsumerConfigs() {
        return consumerConfigs;
    }

    public RocketmqInputConfig setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public Integer getMaxPollRecords() {
        return maxPollRecords;
    }

    public RocketmqInputConfig setMaxPollRecords(Integer maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
        return this;
    }

    public Integer getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public RocketmqInputConfig setConsumeMessageBatchMaxSize(Integer consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
        return this;
    }

    public Integer getWorkThreads() {
        return workThreads;
    }

    public RocketmqInputConfig setWorkThreads(Integer workThreads) {
        this.workThreads = workThreads;
        return this;
    }


    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public RocketmqInputConfig setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
        return this;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public RocketmqInputConfig setValueDeserializer(String valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
        return this;
    }
}
