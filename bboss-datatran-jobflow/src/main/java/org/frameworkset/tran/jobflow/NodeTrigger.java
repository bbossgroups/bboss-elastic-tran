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
import org.frameworkset.tran.jobflow.context.NodeTriggerContext;
import org.frameworkset.tran.jobflow.context.NodeTriggerContextImpl;
import org.frameworkset.tran.jobflow.script.TriggerScriptAPI;
import org.frameworkset.tran.jobflow.script.TriggerScriptUtil;

/**
 * 流程节点执行触发器：可以采用触发器接口和触发器脚本（Groovy）实现条件判断，控制节点是否执行
 * 
 * 可以通过设置动态脚本来计算节点触发条件，返回boolean类型值
 * 亦可以直接设置TriggerScriptAPI来计算节点触发条件，返回boolean类型值
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class NodeTrigger {
    private JobFlowNode jobFlowNode;
    private String triggerScript;
    private TriggerScriptAPI triggerScriptAPI;

    public void setTriggerScriptAPI(TriggerScriptAPI triggerScriptAPI) {
        this.triggerScriptAPI = triggerScriptAPI;
    }

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
    public boolean assertTrigger(JobFlow jobFlow, JobFlowNode jobFlowNode) throws Exception {
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
            NodeTriggerContext nodeTriggerContext = new NodeTriggerContextImpl(jobFlowNode,jobFlow); 
            return triggerScriptAPI.needTrigger(nodeTriggerContext);
        }
        return true;
    }
    
    
}
