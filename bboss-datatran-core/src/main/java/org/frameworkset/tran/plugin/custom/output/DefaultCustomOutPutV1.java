package org.frameworkset.tran.plugin.custom.output;
/**
 * Copyright 2024 bboss
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
 * <p>Description: 兼容旧版本CustomOutPut，不建议使用，请使用CustomOutPutV1</p>
 * <p></p>
 *  
 * @author biaoping.yin
 * @Date 2024/10/24
 */
@Deprecated
public class DefaultCustomOutPutV1 implements CustomOutPutV1{
    private CustomOutPut customOutPut;
    public DefaultCustomOutPutV1(CustomOutPut customOutPut){
        this.customOutPut = customOutPut;
    }

    /**
     * 自定义输出数据方法
     *
     * @param customOutPutContext 封装需要处理的数据和其他作业上下文信息
     */
    @Override
    public void handleData(CustomOutPutContext customOutPutContext) {
        this.customOutPut.handleData(customOutPutContext.getTaskContext(), customOutPutContext.getDatas());
    }
}
