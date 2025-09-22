package org.frameworkset.tran.jobflow.builder;
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
import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.JobFlowNodeFunction;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;

/**
 * @author biaoping.yin
 * @Date 2025/6/22
 */
public class DatatranJobFlowNodeFunction implements JobFlowNodeFunction {
    private JobFlowNode jobFlowNode;
    private ImportBuilder importBuilder;
    private DataStream dataStream;

    protected ImportBuilderFunction importBuilderFunction;
    @Override
    public void init(JobFlowNode jobFlowNode) {
        this.jobFlowNode = jobFlowNode;
        if(importBuilder != null) {
            importBuilder.setJobFlowNode(jobFlowNode);
        }
    }

    public void setImportBuilder(ImportBuilder importBuilder) {
        this.importBuilder = importBuilder;
    }

    public void setImportBuilderFunction(ImportBuilderFunction importBuilderFunction) {
        this.importBuilderFunction = importBuilderFunction;
    }

    @Override
    public Object call(JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {
        ImportBuilder _importBuilder = null;
         if(importBuilderFunction != null){
             _importBuilder = importBuilderFunction.build(jobFlowNodeExecuteContext);
             if(_importBuilder != null) {
                 _importBuilder.setJobFlowNode(jobFlowNode);
             }
         }
         else{
             _importBuilder = importBuilder;
         }
         
         dataStream = _importBuilder.builder(true);
         dataStream.execute();
         return true;
    }

    @Override
    public void reset() {
        dataStream = null;
    }

    @Override
    public void release() {
        if(dataStream != null){
            dataStream = null;
        }
    }

    @Override
    public void pauseSchedule() {
        if(dataStream != null){
            dataStream.pauseSchedule();
        }
    }

    @Override
    public void resumeSchedule() {
        if(dataStream != null){
            dataStream.resumeSchedule();
        }
    }

    @Override
    public void stop() {
        if(dataStream != null){
            dataStream.destroy(true);
        }
    }
}
