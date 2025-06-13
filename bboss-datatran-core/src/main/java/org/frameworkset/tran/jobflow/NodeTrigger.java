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

import org.apache.commons.lang3.StringUtils;
import org.frameworkset.tran.jobflow.script.TriggerScriptAPI;
import org.frameworkset.tran.jobflow.script.TriggerScriptUtil;

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
    private TriggerScriptAPI triggerScriptAPI;
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
    
    private Object lock = new Object();
    public boolean assertTrigger(JobFlow jobFlow) throws Exception {
        //todo 计算条件触发器
        if(triggerScriptAPI == null && StringUtils.isNotEmpty(triggerScript)){
            synchronized (lock) {
                if(triggerScriptAPI == null) {
                    triggerScript = triggerScript.trim();
                    if (triggerScript.length() > 0) {
                        triggerScriptAPI = TriggerScriptUtil.evalTriggerScript(jobFlow,this);

                    }
                }
            }
        }
        if(triggerScriptAPI != null){
            return triggerScriptAPI.evalTriggerScript(jobFlow,jobFlow.getJobFlowExecuteContext());
        }
        return true;
    }
}
