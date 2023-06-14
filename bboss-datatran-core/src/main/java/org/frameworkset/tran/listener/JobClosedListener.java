package org.frameworkset.tran.listener;
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

import org.frameworkset.tran.context.ImportContext;

/**
 * <p>Description: 同步执行作业关闭监听器</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/6/13
 * @author biaoping.yin
 * @version 1.0
 */
public interface JobClosedListener {
    /**
     * 作业关闭监听回调接口方法
     * @param importContext 作业上下文对象，包含作业信息
     * @param throwable 如果作业是因为异常关闭，对应异常对象
     */
    public void jobClosed(ImportContext importContext,Throwable throwable);
}
