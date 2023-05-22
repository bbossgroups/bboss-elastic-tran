package org.frameworkset.tran.config;
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

import java.util.Map;

/**
 * <p>Description: 封装输入参数组，用于并行查询请求处理</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/3/16
 * @author biaoping.yin
 * @version 1.0
 */
public class JobInputParamGroup {
    private Map jobInputParams;

    private Map<String, DynamicParam> jobDynamicInputParams;

    public Map getJobInputParams() {
        return jobInputParams;
    }

    public void setJobInputParams(Map jobInputParams) {
        this.jobInputParams = jobInputParams;
    }

    public Map<String, DynamicParam> getJobDynamicInputParams() {
        return jobDynamicInputParams;
    }

    public void setJobDynamicInputParams(Map<String, DynamicParam> jobDynamicInputParams) {
        this.jobDynamicInputParams = jobDynamicInputParams;
    }
}
