package org.frameworkset.tran.jobflow.schedule;
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.builder.JobFlowBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 工作流外部调度执行器,例如：quartz，xxl-job
 * @Date 2025/6/11
 * @author biaoping.yin
 * @version 1.0
 */
public class ExternalJobFlowScheduler {
	private JobFlowBuilderFunction jobFlowBuilderFunction;
	private JobFlow jobFlow;
	private static Logger logger = LoggerFactory.getLogger(ExternalJobFlowScheduler.class);
//	private Lock lock = new ReentrantLock();

    /**
     * 设置JobFlowBuilder：设置工作流节点和运行参数
     * @param jobFlowBuilderFunction
     */
	public void setJobFlowBuilderFunction(JobFlowBuilderFunction jobFlowBuilderFunction){
		this.jobFlowBuilderFunction = jobFlowBuilderFunction;
	}

    private void initJobFlow(Object params){
        
        if(jobFlow == null) {
            if(jobFlowBuilderFunction == null){
                throw new DataImportException("initJobFlow failed:JobFlowBuilderFunction is null");
            }
            try {
//				lock.lock();
                if(jobFlow == null) {
                    JobFlowBuilder jobFlowBuilder = jobFlowBuilderFunction.build( params);
                    if (!jobFlowBuilder.isExternalTimer())//强制设置为外部定时器模式
                        jobFlowBuilder.setExternalTimer(true);
//					if(db2ESImportBuilder.isAsyn()){//强制设置为同步等待模式
//						db2ESImportBuilder.setAsyn(false);
//					}
                    jobFlow = jobFlowBuilder.build();
                }
            }
            catch (DataImportException e){
                if(logger.isErrorEnabled())
                    logger.error("ExternalJobScheduler execute failed:",e);
                throw e;
            }
            catch (Exception e){
                if(logger.isErrorEnabled())
                    logger.error("ExternalJobScheduler execute failed:",e);
                throw new DataImportException("ExternalJobScheduler execute failed:",e);
            }
            catch (Throwable e){
                if(logger.isErrorEnabled())
                    logger.error("ExternalJobScheduler execute failed:",e);
                throw new DataImportException("ExternalJobScheduler execute failed:",e);
            }
            finally {
//				lock.unlock();
            }
            if(jobFlow == null)
            {
                throw new DataImportException("ExternalJobScheduler failed: jobFlow build failed");
            }
            jobFlow.initJob();

//			dataStream.init();

        }
    }
	public void execute(Object params){
        initJobFlow(  params);
        jobFlow.execute();
	}

	public void destroy(){
		if(this.jobFlow != null){
			this.jobFlow.stop();
            jobFlow = null;
		}
        jobFlowBuilderFunction = null;

	}
}
