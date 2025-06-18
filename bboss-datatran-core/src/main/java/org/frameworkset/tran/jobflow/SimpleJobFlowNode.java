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

import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.jobflow.context.JobFlowExecuteContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class SimpleJobFlowNode extends JobFlowNode{
    private static Logger logger = LoggerFactory.getLogger(SimpleJobFlowNode.class);
 
    /**
     * 串行节点作业配置
     */
    private ImportBuilder importBuilder;
 
 

    private DataStream dataStream;
    public SimpleJobFlowNode(ImportBuilder importBuilder, NodeTrigger nodeTrigger ){
        this(importBuilder);
        this.nodeTrigger = nodeTrigger;
    }

    public SimpleJobFlowNode(ImportBuilder importBuilder){
        this.importBuilder = importBuilder;
        if(importBuilder == null){
            throw new JobFlowException("ImportBuilder is null.");
        }
        this.importBuilder.setJobFlowNode(this);
    }

 
    
    /**
     * 启动流程当前节点
     */
    @Override
    public boolean start(){
        increament();
        if(this.assertTrigger()) {
            logger.info("Start SimpleJobFlowNode[id={},name={}] begin.",this.getNodeId(),this.getNodeName());
            dataStream = importBuilder.builder(true);
            dataStream.execute();
            
            logger.info("Execute SimpleJobFlowNode[id={},name={}] complete.",this.getNodeId(),this.getNodeName());
//            this.nodeComplete(null);
            return true;
        }
        else{
            logger.info("AssertTrigger: false,ignore execute this SimpleJobFlowNode[id={},name={}].",this.getNodeId(),this.getNodeName());
            nodeComplete(null);
        }
        return false;
    }

    /**
     * 停止流程当前节点
     */
    @Override
    public void stop(){
        JobFlowExecuteContext jobFlowExecuteContext = jobFlow.getJobFlowExecuteContext();
        
       
        if(dataStream != null){
            dataStream.destroy(true);
        }
        logger.info("Stop SimpleJobFlowNode[id="+this.getNodeId()+",name="+this.getNodeName()+"] complete.");
        if(this.nextJobFlowNode != null){
            try {
                this.nextJobFlowNode.stop();
//                logger.warn("Stop nextJobFlowNode of["+this.getNodeName()+"] complete.");
            }
            catch (Exception e){
                logger.warn("Stop nextJobFlowNode[id="+nextJobFlowNode.getNodeId()+",name="+nextJobFlowNode.getNodeName()+"] failed:",e);
            }
        }
    }

    /**
     * 暂停流程节点
     */
    @Override
    public void pause(){
        JobFlowExecuteContext jobFlowExecuteContext = jobFlow.getJobFlowExecuteContext();
        if(dataStream != null){
            dataStream.pauseSchedule();
        }
    }

    /**
     * 唤醒暂停流程节点
     */
    @Override
    public void consume() {
        JobFlowExecuteContext jobFlowExecuteContext = jobFlow.getJobFlowExecuteContext();
        if(dataStream != null){
            dataStream.resumeSchedule();
        }
    }

 

    
    
}
