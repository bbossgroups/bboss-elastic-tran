package org.frameworkset.tran.kafka.output;
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
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.BaseImportContext;
import org.frameworkset.tran.util.JsonReocordGenerator;

import java.io.Writer;
import java.util.Properties;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/25 15:18
 * @author biaoping.yin
 * @version 1.0
 */
public class KafkaOutputContextImpl extends BaseImportContext implements KafkaOutputContext{
	private KafkaOutputConfig kafkaOutputConfig;

	public KafkaSend getKafkaSend() {
		return kafkaSend;
	}

	private KafkaSend kafkaSend;
	public KafkaOutputContextImpl(BaseImportConfig kafkaOutputConfig){
		super(kafkaOutputConfig);

	}
	public boolean kafkaAsynSend(){
		return kafkaOutputConfig.isKafkaAsynSend();
	}
	@Override
	public void init(){
		super.init();
		this.kafkaOutputConfig = (KafkaOutputConfig)baseImportConfig;
		if(kafkaOutputConfig.getReocordGenerator() == null){
			kafkaOutputConfig.setReocordGenerator(new JsonReocordGenerator());
		}


	}

	public void setDataTranPlugin(DataTranPlugin dataTranPlugin) {
		super.setDataTranPlugin(dataTranPlugin);
		kafkaSend = (KafkaSend)this.getDataTranPlugin();
	}

	@Override
	public String getTopic() {
		return kafkaOutputConfig.getTopic();
	}

	@Override
	public Properties getKafkaConfigs() {
		return kafkaOutputConfig.getKafkaConfigs();
	}
	public void generateReocord(org.frameworkset.tran.context.Context context, CommonRecord record, Writer builder){
		kafkaOutputConfig.getReocordGenerator().buildRecord(context,record,builder);
	}
}
