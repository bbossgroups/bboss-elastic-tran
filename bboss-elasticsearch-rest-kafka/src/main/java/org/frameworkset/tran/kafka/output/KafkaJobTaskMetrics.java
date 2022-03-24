package org.frameworkset.tran.kafka.output;
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

import org.frameworkset.tran.metrics.JobTaskMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/3/24
 * @author biaoping.yin
 * @version 1.0
 */
public class KafkaJobTaskMetrics extends JobTaskMetrics {
	private Date jobAwaitEndTime;
	private static Logger logger = LoggerFactory.getLogger(KafkaJobTaskMetrics.class);
	@Override
	public void await(){
		if(totalRecords < tasks){
			do{
				try {
					Thread.currentThread().sleep(5000);
				} catch (InterruptedException e) {
					logger.warn("",e);
					break;
				}
				if(totalRecords >= tasks){
					break;
				}
			}while(true);
			jobAwaitEndTime = new Date();
		}

	}

	@Override
	public void await(long waitime){
		if(waitime <=0) {
			await();
			return;
		}
		if(totalRecords < tasks){
			long startTime = System.currentTimeMillis();
			do{
				try {
					Thread.currentThread().sleep(5000);
				} catch (InterruptedException e) {
					logger.warn("",e);
					break;
				}
				if(totalRecords >= tasks){
					break;
				}
				if(System.currentTimeMillis() - startTime >= waitime){//达到超时时间，直接退出
					break;
				}
			}while(true);
			jobAwaitEndTime = new Date();
		}
	}
	@Override
	protected void buildString(StringBuilder builder){
		super.buildString(builder);
		if(jobAwaitEndTime != null){
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			builder.append(",jobAwaitEndTime:").append(dateFormat.format(jobAwaitEndTime));

		}
		else {
			builder.append(",jobAwaitEndTime:-");
		}

	}
}
