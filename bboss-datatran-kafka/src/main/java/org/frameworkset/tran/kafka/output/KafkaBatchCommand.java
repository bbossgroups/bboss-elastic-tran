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

import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.plugin.kafka.output.KafkaOutputConfig;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.frameworkset.tran.context.Context.KAFKA_TOPIC_KEY;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/26 17:07
 * @author biaoping.yin
 * @version 1.0
 */
public class KafkaBatchCommand extends BaseTaskCommand<  Object> {
    private static Logger logger = LoggerFactory.getLogger(KafkaBatchCommand.class);
	private KafkaOutputConfig kafkaOutputConfig;
	public KafkaBatchCommand(TaskCommandContext taskCommandContext) {
		super(  taskCommandContext);
		kafkaOutputConfig = (KafkaOutputConfig) importContext.getOutputConfig();
	}

    @Override
	public Object execute() throws Exception {
        if(records.size() > 0) {
            Object key = null;
            Object datas = null;
            String topic = null;
            StringBuilder builder = new StringBuilder();
            BBossStringWriter writer = new BBossStringWriter(builder);

            for (int i = 0; i < records.size(); i++) {
                CommonRecord record = records.get(i);
                /**
                 * 从临时变量中获取记录对应的kafka主题，如果存在，就用记录级别的kafka主题，否则用全局配置的kafka主题发送数据
                 */
                Object topic_ = null;
                key = record.getRecordKey();
                /**
                 * 从临时变量中获取记录对应的kafka主题，如果存在，就用记录级别的kafka主题，否则用全局配置的kafka主题发送数据
                 */
                topic_ = record.getTempData(KAFKA_TOPIC_KEY);
                if (topic_ != null && topic_ instanceof String) {
                    if (!topic_.equals(""))
                        topic = (String) topic_;
                    else {
                        topic = kafkaOutputConfig.getTopic();
                    }
                } else {
                    topic = kafkaOutputConfig.getTopic();
                }

                kafkaOutputConfig.getRecordGenerator().buildRecord(taskContext, record, writer);
                datas = writer.toString();
                builder.setLength(0);
                kafkaOutputConfig.getKafkaSend().batchSend(this, taskContext, topic, key, datas);
            }
            builder.setLength(0);
        }
        else{
            if (logger.isInfoEnabled()){
                logger.info("All output data is ignored and do nothing.");
            }
        }
        finishTask();
		 return null;
	}


	 

}
