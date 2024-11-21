package org.frameworkset.tran.plugin.rocketmq.output;
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

import org.frameworkset.rocketmq.RocketmqProductor;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.JobTaskMetrics;
import org.frameworkset.tran.plugin.BasePlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.rocketmq.output.RocketmqJobTaskMetrics;
import org.frameworkset.tran.rocketmq.output.RocketmqOutputDataTran;
import org.frameworkset.tran.rocketmq.output.RocketmqSendImpl;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseTaskCommand;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class RocketmqOutputDataTranPlugin extends BasePlugin implements OutputPlugin{
	private RocketmqProductor rocketmqProductor;
	private RocketmqOutputConfig rocketmqOutputConfig;
	@Override
	public void init(){

	}

	@Override
	public void destroy(boolean waitTranStop) {
		if(rocketmqProductor != null){
            rocketmqProductor.destroy();
		}
	}

	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, JobCountDownLatch countDownLatch, Status currentStatus){
        RocketmqOutputDataTran kafkaOutputDataTran = new RocketmqOutputDataTran(  taskContext,jdbcResultSet, importContext,countDownLatch,  currentStatus);
		kafkaOutputDataTran.initTran();
		return kafkaOutputDataTran;
	}


	public RocketmqOutputDataTranPlugin(ImportContext importContext){
		super(importContext);
        rocketmqOutputConfig = (RocketmqOutputConfig) importContext.getOutputConfig();
	}

	@Override
	public void afterInit() {
	}

	@Override
	public void beforeInit() {

	}
    public void batchSend(final BaseTaskCommand taskCommand, TaskContext taskContext, String topic,Object key, Object data) {
        if(rocketmqProductor == null){
            synchronized (this) {
                if(rocketmqProductor == null) {
                    rocketmqProductor = RocketmqSendImpl.buildProducer( rocketmqOutputConfig);

                }
            }
        }
        RocketmqSendImpl.batchSend(rocketmqProductor,rocketmqOutputConfig,taskCommand,taskContext,topic,key,data);
    }

	 
	@Override
	public JobTaskMetrics createJobTaskMetrics(){
//		return getOutputPlugin().createJobTaskMetrics();
        if(importContext.isSerial()) {
            return new RocketmqJobTaskMetrics();
        }
        else{
            return super.createJobTaskMetrics();
        }
	}
}
