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

import org.frameworkset.tran.jobflow.JobFlowNode;
import org.frameworkset.tran.jobflow.JobFlowNodeStatus;
import org.frameworkset.tran.jobflow.JobFlowStatus;

/**
 * @author biaoping.yin
 * @Date 2025/6/20
 */
public interface NodeTriggerContext {
    StaticContext getJobFlowStaticContext();
    JobFlowExecuteContext getJobFlowExecuteContext();
    JobFlowStatus getJobFlowStatus();
    JobFlowNode getRunningJobFlowNode();
    StaticContext getPreJobFlowStaticContext();
    JobFlowNode getJobFlowNode();
    JobFlowNodeStatus getPreJobFlowNodeStatus();
    Object getFlowContextData(String name) ;
    Object getFlowContextData(String name,Object defaultValue) ;
    Object getContainerContextData(String name) ;
    Object getContainerContextData(String name,Object defaultValue) ;
    Object getContainerContextData(String name,boolean scanParant);
    Object getContainerContextData(String name,Object defaultValue,boolean scanParant);

}
