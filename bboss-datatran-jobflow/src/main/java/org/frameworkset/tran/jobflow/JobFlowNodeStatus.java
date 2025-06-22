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
 * 工作流节点运行状态
 * @author biaoping.yin
 * @Date 2025/6/15
 */
public enum JobFlowNodeStatus {
    /**
     * 初始状态
     */
    INIT,
    /**
     * 启动状态
     */
    STARTED,
    /**
     * 停止中状态
     */
    STOPPING,
    /**
     * 已停止状态
     */
    STOPED,

    /**
     * 暂停状态
     */
    PAUSE,
    /**
     * 从暂停恢复到运行状态
     */
    RUNNING,

    /**
     * 已完成
     */
    COMPLETE

}
