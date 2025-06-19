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

import org.frameworkset.tran.jobflow.ParrelJobFlowNode;

/**
 * 用于跟踪和记录并行分支节点执行情况
 * @author biaoping.yin
 * @Date 2025/6/18
 */
public class ParrelJobFlowNodeContext extends StaticContext{
    private ParrelJobFlowNode parrelJobFlowNode;
   
    public ParrelJobFlowNodeContext(ParrelJobFlowNode parrelJobFlowNode){
        super();
        this.parrelJobFlowNode = parrelJobFlowNode;
       
    }


    public ParrelJobFlowNode getParrelJobFlowNode() {
        return parrelJobFlowNode;
    }

    /**
     * 节点完成时，减少启动节点计数,完成计数器加1
     * @return
     */
    public int nodeComplete() {
        return super.nodeComplete();
    }
}
