package org.frameworkset.tran.jobflow;
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

/**
 * @author biaoping.yin
 * @Date 2026/5/19
 */
public class ExecuteResult {
    /**
     * 是否忽略后续节点执行 true 忽略 false 不忽略
     */
    private boolean ignoreNextNodeExecute;


    /**
     * 执行失败标记：true 失败  false 正常
     */
    private boolean failed;
    private JobFlowNode nextNode;
    /**
     * 是否执行成功，true 表示执行成功 false 表示执行失败或者忽略执行
     */
    private boolean resultFlag;

    public boolean isIgnoreNextNodeExecute() {
        return ignoreNextNodeExecute;
    }

    public void setIgnoreNextNodeExecute(boolean ignoreNextNodeExecute) {
        this.ignoreNextNodeExecute = ignoreNextNodeExecute;
    }

    public JobFlowNode getNextNode() {
        return nextNode;
    }

    public void setNextNode(JobFlowNode nextNode) {
        this.nextNode = nextNode;
    }
    
    public boolean isResultFlag() {
        return resultFlag;
    }
    public void setResultFlag(boolean resultFlag) {
        this.resultFlag = resultFlag;
    }

    public boolean isFailed() {
        return failed;
    }

    public void setFailed(boolean failed) {
        this.failed = failed;
    }
}
