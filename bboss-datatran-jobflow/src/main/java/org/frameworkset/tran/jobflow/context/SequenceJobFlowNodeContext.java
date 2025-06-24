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

/**
 * 用于跟踪串行分支节点执行情况
 * @author biaoping.yin
 * @Date 2025/6/18
 */
public class SequenceJobFlowNodeContext extends JobFlowNodeContext {
    private SequenceJobFlowNode sequenceJobFlowNode;

    /**
     * 当前正在执行的作业节点
     */
    private JobFlowNode runningJobFlowNode;

    private Object runningJobFlowNodeLock = new Object();

    public SequenceJobFlowNodeContext(SequenceJobFlowNode sequenceJobFlowNode){
        super();
        this.sequenceJobFlowNode = sequenceJobFlowNode;
        this.jobFlowNode = sequenceJobFlowNode;
    }
    
    public void reset(){
        synchronized (runningJobFlowNodeLock) {
            this.runningJobFlowNode = null;
        }
        super.reset();
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

    public void pause() {
        synchronized (runningJobFlowNodeLock) {
            if(runningJobFlowNode != null){
                this.runningJobFlowNode.pause();
            }
        }
    }
    public void consume() {
        synchronized (runningJobFlowNodeLock) {
            if(runningJobFlowNode != null){
                this.runningJobFlowNode.consume();
            }
        }
    }

    /**
     * 工作流或者复合节点（串行/并行）子节点完成时，减少启动节点计数,完成计数器加1
     * @param throwable 子节点触发的异常
     * @param jobFlowNode 完成的子节点
     * @return
     */
    @Override
    public int nodeComplete(Throwable throwable,JobFlowNode jobFlowNode) {
        this.runningJobFlowNode = null;
        return super.nodeComplete(throwable,jobFlowNode);

    }
}
