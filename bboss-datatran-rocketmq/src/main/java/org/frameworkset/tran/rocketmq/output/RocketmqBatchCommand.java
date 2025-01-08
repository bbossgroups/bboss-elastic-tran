package org.frameworkset.tran.rocketmq.output;
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
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.plugin.rocketmq.output.RocketmqOutputConfig;
import org.frameworkset.tran.plugin.rocketmq.output.RocketmqOutputDataTranPlugin;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
import org.frameworkset.tran.util.RecordGeneratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.frameworkset.tran.context.Context.ROCKETMQ_TOPIC_KEY;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/26 17:07
 * @author biaoping.yin
 * @version 1.0
 */
public class RocketmqBatchCommand extends BaseTaskCommand<  Object> {
    private static Logger logger = LoggerFactory.getLogger(RocketmqBatchCommand.class);
	private RocketmqOutputConfig rocketmqOutputConfig;
    private RocketmqOutputDataTranPlugin rocketmqOutputDataTranPlugin;
	public RocketmqBatchCommand(TaskCommandContext taskCommandContext, OutputConfig outputConfig) {
		super( outputConfig, taskCommandContext);
        rocketmqOutputConfig = (RocketmqOutputConfig) outputConfig;
        rocketmqOutputDataTranPlugin = (RocketmqOutputDataTranPlugin) outputConfig.getOutputPlugin();
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
                topic_ = record.getTempData(ROCKETMQ_TOPIC_KEY);
                if (topic_ != null && topic_ instanceof String) {
                    if (!topic_.equals(""))
                        topic = (String) topic_;
                    else {
                        topic = rocketmqOutputConfig.getTopic();
                    }
                } else {
                    topic = rocketmqOutputConfig.getTopic();
                }
                RecordGeneratorContext recordGeneratorContext = new RecordGeneratorContext();
                recordGeneratorContext.setRecord(record);
                recordGeneratorContext.setTaskContext(taskContext);
                recordGeneratorContext.setBuilder(writer);
                recordGeneratorContext.setTaskMetrics(taskMetrics);

                rocketmqOutputConfig.getRecordGeneratorV1().buildRecord(  recordGeneratorContext);
//                kafkaOutputConfig.getRecordGeneratorV1().buildRecord(taskContext,taskMetrics, record, writer);
                datas = writer.toString();
                builder.setLength(0);
                rocketmqOutputDataTranPlugin.batchSend(this, taskContext, topic, key, datas);
            }
            builder.setLength(0);
        }
        else{
            logNodatas( logger);
        }
        finishTask();
		 return null;
	}


	 

}
