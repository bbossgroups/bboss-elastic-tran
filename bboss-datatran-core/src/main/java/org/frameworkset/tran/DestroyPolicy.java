package org.frameworkset.tran;
/**
 * Copyright 2023 bboss
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
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/6/29
 * @author biaoping.yin
 * @version 1.0
 */
public class DestroyPolicy {
       /**
        * waitTranStopped true 等待同步作业处理完成后停止作业 false 不等待
        */
    private boolean waitTranStopped;
    /**
     * fromScheduleEnd 销毁操作是否来自于自动停止作业操作
     */
    private boolean fromScheduleEnd;
    /**
     * 是否强制关闭,如果是强制关闭，不会等待后续数据处理完毕，直接快速退出处理
     */
    private boolean forceStop;

    public boolean isWaitTranStopped() {
        return waitTranStopped;
    }

    public DestroyPolicy setWaitTranStopped(boolean waitTranStopped) {
        this.waitTranStopped = waitTranStopped;
        return this;
    }

    public boolean isFromScheduleEnd() {
        return fromScheduleEnd;
    }

    public DestroyPolicy setFromScheduleEnd(boolean fromScheduleEnd) {
        this.fromScheduleEnd = fromScheduleEnd;
        return this;
    }

    public boolean isForceStop() {
        return forceStop;
    }

    public DestroyPolicy setForceStop(boolean forceStop) {
        this.forceStop = forceStop;
        return this;
    }
}
