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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 组合流程节点，可以由多个节点：simple，Sequence，Parrel节点组合的复杂流程分支节点
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public abstract class CompositionJobFlowNode extends JobFlowNode{
    private static Logger logger = LoggerFactory.getLogger(CompositionJobFlowNode.class);
    /**
     * 并行节点作业配置
     */
    protected List<JobFlowNode> jobFlowNodes;

   

 

    /**
     * 分支完成
     * @param jobFlowNode
     */
    public abstract void brachComplete(JobFlowNode jobFlowNode,  Throwable e) ;
//    {
//        int liveNodes = this.staticContext.decreament();
//        if(liveNodes <= 0){
////            this.nodeComplete( e);
//        }
//    }

    
}
