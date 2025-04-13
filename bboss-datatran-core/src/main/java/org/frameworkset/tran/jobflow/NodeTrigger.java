package org.frameworkset.tran.jobflow;
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

/**
 * <p>Description: 流程节点执行触发器</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class NodeTrigger {
    private JobFlowNode jobFlowNode;
    private String triggerScript;
    public String getTriggerScript() {
        return triggerScript;
    }

    public void setTriggerScript(String triggerScript) {
        this.triggerScript = triggerScript;
    }

    public JobFlowNode getJobFlowNode() {
        return jobFlowNode;
    }

    public void setJobFlowNode(JobFlowNode jobFlowNode) {
        this.jobFlowNode = jobFlowNode;
    }

    public boolean assertTrigger(JobFlow jobFlow) {
        JobFlowExecuteContext jobFlowExecuteContext = jobFlow.getJobFlowExecuteContext();
        //todo 计算条件触发器
        return true;
    }
}
