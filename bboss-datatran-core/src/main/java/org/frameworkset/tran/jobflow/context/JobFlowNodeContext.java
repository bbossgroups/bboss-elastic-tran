package org.frameworkset.tran.jobflow.context;
/**
 * Copyright 2025 bboss
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

import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.JobFlowNodeStatus;

/**
 * 节点运行情况统计上下文
 * @author biaoping.yin
 * @Date 2025/6/18
 */
public class JobFlowNodeContext extends StaticContext{
     protected JobFlowNodeStatus jobFlowNodeStatus = JobFlowNodeStatus.INIT;
     protected JobFlowNode jobFlowNode;
     private Object updateJobFlowNodeStatusLock = new Object();
    public JobFlowNodeStatus updateJobFlowNodeStatus(JobFlowNodeStatus jobFlowNodeStatus){
        synchronized (updateJobFlowNodeStatusLock){
            this.jobFlowNodeStatus = jobFlowNodeStatus;
            return jobFlowNodeStatus;
        }
       
    }
    
    public boolean assertStoped(){
        synchronized (updateJobFlowNodeStatusLock){
            return jobFlowNodeStatus == JobFlowNodeStatus.STOPED || jobFlowNodeStatus == JobFlowNodeStatus.STOPPING;
        }
        
    }

    public JobFlowNode getJobFlowNode() {
        return jobFlowNode;
    }

    public JobFlowNodeStatus getJobFlowNodeStatus() {
        synchronized (updateJobFlowNodeStatusLock){
            return jobFlowNodeStatus;
        }
    }

    public void reset(){
        synchronized (updateJobFlowNodeStatusLock){
            jobFlowNodeStatus = JobFlowNodeStatus.INIT;
        }
        super.reset();
    }
}
