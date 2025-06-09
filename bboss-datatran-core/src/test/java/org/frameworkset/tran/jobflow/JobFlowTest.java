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

import org.frameworkset.tran.config.ImportBuilder;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/4/2
 */
public class JobFlowTest {
    public static void main(String[] args){
        JobFlowBuilder jobFlowBuilder = new JobFlowBuilder();
        jobFlowBuilder.setJobFlowName("测试流程")
                .setJobFlowId("测试id");
        /**
         * 1.构建第一个任务节点：单任务节点
         */
        ComJobFlowNodeBuilder jobFlowNodeBuilder = new ComJobFlowNodeBuilder();
        /**
         * 1.1 为第一个任务节点添加一个带触发器的作业
         */
        jobFlowNodeBuilder.buildImportBuilder(new ImportBuilderCreate() {
            @Override
            public ImportBuilder createImportBuilder(JobFlowNodeBuilder jobFlowNodeBuilder) {
                return null;
            }
        }, new NodeTriggerCreate() {
            @Override
            public NodeTrigger createNodeTrigger(JobFlowNodeBuilder jobFlowNodeBuilder) {
                return null;
            }
        });
        /**
         * 1.2 将第一个节点添加到工作流构建器
         */
        jobFlowBuilder.addJobFlowNode(jobFlowNodeBuilder);
        
        /**
         * 2.构建第二个任务节点：并行任务节点
         */
        ParrelJobFlowNodeBuilder parrelJobFlowNodeBuilder = new ParrelJobFlowNodeBuilder();
        /**
         * 2.1 为第二个并行任务节点添加第一个带触发器的作业任务
         */
        parrelJobFlowNodeBuilder.addImportBuilder(new ImportBuilderCreate() {
            @Override
            public ImportBuilder createImportBuilder(JobFlowNodeBuilder jobFlowNodeBuilder) {
                return null;
            }
        }, new NodeTriggerCreate() {
            @Override
            public NodeTrigger createNodeTrigger(JobFlowNodeBuilder jobFlowNodeBuilder) {
                return null;
            }
        });
        /**
         * 2.2 为第二个并行任务节点添加第二个不带触发器的作业任务
         */
        parrelJobFlowNodeBuilder.addImportBuilder(new ImportBuilderCreate() {
            @Override
            public ImportBuilder createImportBuilder(JobFlowNodeBuilder jobFlowNodeBuilder) {
                return null;
            }
        });
        /**
         * 2.3 为第二个并行任务节点添加第三个串行复杂流程子任务
         */
        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(new ComJobFlowNodeBuilder());

        /**
         * 2.4 为第二个并行任务节点添加第三个并行行复杂流程子任务
         */
        parrelJobFlowNodeBuilder.addJobFlowNodeBuilder(new ParrelJobFlowNodeBuilder());

        /**
         * 2.5 将第二个节点添加到工作流中
         */
        jobFlowBuilder.addJobFlowNode(parrelJobFlowNodeBuilder);
        
        JobFlow jobFlow = jobFlowBuilder.build();
        jobFlow.start();
        
        jobFlow.stop();

        jobFlow.pause();
        
        jobFlow.consume();
        

    }
}
