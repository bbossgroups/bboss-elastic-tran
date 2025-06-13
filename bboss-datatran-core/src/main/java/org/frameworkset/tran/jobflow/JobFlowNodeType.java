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

/**
 * 流程节点类型
 * @author biaoping.yin
 * @Date 2025/6/13
 */
public enum JobFlowNodeType {
    /**
     * 简单类型节点，只有一个作业节点，可以在工作流，亦可以出现在顺序或者并行执行的复合类型节点中
     */
    SIMPLE,
    /**
     * 顺序执行的复合类型节点，节点清单中所有节点将串行执行
     */
    SEQUENCE,
    /**
     * 并行执行的复合类型节点，节点清单中所有节点将并行执行
     */
    PARREL
}
