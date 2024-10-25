package org.frameworkset.tran.plugin.custom.output;
/**
 * Copyright 2020 bboss
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
 * <p>Description: 自定义数据处理组件</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/10/9 17:08
 * @author biaoping.yin
 * @version 1.0
 */
public interface CustomOutPutV1 {
//    @Deprecated
//    /**
//     * 不建议方法
//     * 使用handleData(TaskContext taskContext, TaskMetrics taskMetrics,List<CommonRecord> datas)
//     */
//	default public void handleData(TaskContext taskContext,List<CommonRecord> datas){
//        
//    }

    /**
     * 自定义输出数据方法
     * @param customOutPutContext 封装需要处理的数据和其他作业上下文信息
     */
    public void handleData(CustomOutPutContext customOutPutContext);
}
