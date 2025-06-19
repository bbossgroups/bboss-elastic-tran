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

/**
 * 节点运行情况统计上下文
 * @author biaoping.yin
 * @Date 2025/6/18
 */
public class StaticContext {
    private int runningNodes;
    private int startNodes ;
    private int completeNodes;
    private Object updateLock = new Object();
    public StaticContext(){
        
    }

    /**
     * 拷贝节点状态信息
     * @return
     */
    public StaticContext copy(){
        synchronized (updateLock) {
            StaticContext staticContext = new StaticContext();
            staticContext.startNodes = this.startNodes;
            staticContext.completeNodes = this.completeNodes;
            staticContext.runningNodes = this.runningNodes;
            return staticContext;
        }
    }

    /**
     * 重置计数器
     */
    public void reset() {
        synchronized (updateLock) {
            startNodes = 0;
            completeNodes = 0;
            runningNodes = 0;
        }
    }

    /**
     * 获取所有已经启动的节点
     * @return
     */
    public int getStartNodes() {
        synchronized (updateLock) {
            return startNodes;
        }
    }

    /**
     * 节点启动时，增加启动节点计数
     * @return
     */
    public  int nodeStart() {
        synchronized (updateLock) {
            startNodes ++;
            runningNodes ++;
            return startNodes;
        }
    }

    /**
     * 节点完成时，减少启动节点计数,完成计数器加1
     * @return
     */
    public int nodeComplete() {
        synchronized (updateLock) {
            runningNodes --;//正在运行节点数量递减
            completeNodes++;
            return completeNodes;
        }
    }

    /**
     * 获取运行中的节点
     * @return
     */
    public int getRunningNodes(){
        synchronized (updateLock) {
            return runningNodes;
        }
    }

    /**
     * 获取已经执行完成的节点
     * @return
     */
    public int getCompleteNodes(){
        synchronized (updateLock) {
            return completeNodes;
        }
    }

    /**
     * 判断所有启动的节点是否都已经执行完成
     * @return
     */
    public boolean allNodeComplete(){
        synchronized (updateLock) {
            return completeNodes == startNodes;
        }
    }
}
