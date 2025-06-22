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

import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.jobflow.JobFlowNodeFunction;

/**
 * 数据交换作业节点构建器
 * @author biaoping.yin
 * @Date 2025/3/31
 */
public class DatatranJobFlowNodeBuilder extends SimpleJobFlowNodeBuilder {


 

    /**
     * 串行节点作业配置
     */
    protected ImportBuilder importBuilder;

    public DatatranJobFlowNodeBuilder(String nodeId, String nodeName) {
        super(nodeId, nodeName);        
    }

    @Override
    protected JobFlowNodeFunction buildJobFlowNodeFunction() {
        DatatranJobFlowNodeFunction datatranJobFlowNodeFunction = new DatatranJobFlowNodeFunction()  ;
        datatranJobFlowNodeFunction.setImportBuilder(importBuilder);
        return datatranJobFlowNodeFunction;
    }


    public DatatranJobFlowNodeBuilder setImportBuilder(ImportBuilder importBuilder) {
        this.importBuilder = importBuilder;
        return this;
    }

    public ImportBuilder getImportBuilder() {
        return importBuilder;
    }

}
