package org.frameworkset.tran.jobflow.builder;
/**
 * Copyright 2026 bboss
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

import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.jobflow.JobFlowNodeFunction;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;

/**
 * @author biaoping.yin
 * @Date 2026/3/23
 */
public abstract class SimpleDatatranJobFlowNodeBuilder extends DatatranJobFlowNodeBuilder{
    public SimpleDatatranJobFlowNodeBuilder(String nodeId, String nodeName) {
        super(nodeId, nodeName);
    }

    public SimpleDatatranJobFlowNodeBuilder(String nodeName) {
        super(nodeName);
    }

    public SimpleDatatranJobFlowNodeBuilder() {
    }
    
    protected abstract ImportBuilder buildImportBuilder(JobFlowNodeExecuteContext jobFlowNodeExecuteContext);

    @Override
    protected JobFlowNodeFunction buildJobFlowNodeFunction() {
        DatatranJobFlowNodeFunction datatranJobFlowNodeFunction = new DatatranJobFlowNodeFunction()  ;
//        datatranJobFlowNodeFunction.setImportBuilder(importBuilder);
        datatranJobFlowNodeFunction.setImportBuilderFunction(jobFlowNodeExecuteContext -> buildImportBuilder(jobFlowNodeExecuteContext));
        return datatranJobFlowNodeFunction;
    }
}
