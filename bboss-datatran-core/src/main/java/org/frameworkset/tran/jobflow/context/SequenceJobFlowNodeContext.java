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
import org.frameworkset.tran.jobflow.SequenceJobFlowNode;
import org.frameworkset.util.concurrent.IntegerCount;

/**
 * 用于跟踪串行分支节点执行情况
 * @author biaoping.yin
 * @Date 2025/6/18
 */
public class SequenceJobFlowNodeContext extends StaticContext{
    private SequenceJobFlowNode sequenceJobFlowNode;

    /**
     * 当前正在执行的作业节点
     */
    private JobFlowNode runningJobFlowNode;

    private Object runningJobFlowNodeLock = new Object();

    public SequenceJobFlowNodeContext(SequenceJobFlowNode sequenceJobFlowNode){
        super();
        this.sequenceJobFlowNode = sequenceJobFlowNode;
       
    }
   

    public SequenceJobFlowNode getSequenceJobFlowNode() {
        return sequenceJobFlowNode;
    }

    public void setRunningJobFlowNode(JobFlowNode runningJobFlowNode) {
        synchronized (runningJobFlowNodeLock) {
            this.runningJobFlowNode = runningJobFlowNode;
        }
    }

    public JobFlowNode getRunningJobFlowNode() {
        synchronized (runningJobFlowNodeLock) {
            return runningJobFlowNode;
        }
    }

    public void stop() {
        synchronized (runningJobFlowNodeLock) {
            if(runningJobFlowNode != null){
                this.runningJobFlowNode.stop();
            }
        }
    }
}
