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

import org.frameworkset.util.concurrent.IntegerCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 顺序执行的复合流程节点
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class SequenceJobFlowNode extends CompositionJobFlowNode{
    private static Logger logger = LoggerFactory.getLogger(SequenceJobFlowNode.class);
   

    /**
     * 启动流程当前节点
     */
    @Override
    public boolean start(){
        
        startNodes = new IntegerCount();
        if(assertTrigger()) {
            if (jobFlowNodes == null || jobFlowNodes.size() == 0) {
                throw new JobFlowException("ParrelJobFlowNode must set jobFlowNodes,please set jobFlowNodes first.");
            } else {
                for (int i = 0; i < jobFlowNodes.size(); i++) {
                    JobFlowNode jobFlowNode = jobFlowNodes.get(i);
                    if(jobFlowNode.start())
                        startNodes.increament();
                }
            }
            return true;
        }
        return false;
        
    }

    /**
     * 停止流程当前节点
     */
    @Override
    public void stop() {
        for (int i = 0; jobFlowNodes != null && i < jobFlowNodes.size(); i++) {
            JobFlowNode jobFlowNode = jobFlowNodes.get(i);
            jobFlowNode.stop();
        }
    }

   
}
