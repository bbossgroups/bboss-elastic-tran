package org.frameworkset.tran.record;
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
 * <p>Description: 数据迭代器检测是否还有数据的判断结果</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/6/28
 * @author biaoping.yin
 * @version 1.0
 */
public class NextAssert {
    /**
     * 存在数据则为true，否则为false
     *
     */
    private boolean hasNext;
    /**
     * 是否达到强制刷数据的时间间隔
     * 如果设置了batchsize，处理逻辑是这样的：
     * 1.当到来的数据量达到batchsize时，parallelBatchExecute 就生成一个持久化任务命令就像数据输出
     * 2.当有数据或者drop忽略掉的数据，数据量没有达到batchsize，但是已经过了等待的最大时间间隔，
     *   parallelBatchExecute 就生成一个持久化任务命令就像数据输出
     */
    private boolean needFlush;

    public boolean isHasNext() {
        return hasNext;
    }

    public void setHasNext(boolean hasNext) {
        this.hasNext = hasNext;
    }

    public boolean isNeedFlush() {
        return needFlush;
    }

    public void setNeedFlush(boolean needFlush) {
        this.needFlush = needFlush;
    }
}
