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

import java.util.Map;

/**
 * @author biaoping.yin
 * @Date 2026/5/12
 */
public class JobParams {
    private Map<String,Object> params;
    public Map<String, Object> getParams() {
        return params;
    }
    public JobParams addParam(String name,Object value){
        if(params == null){
            params = new java.util.LinkedHashMap<>();
        }
        params.put(name,value);
        return this;
    }
    public void setParams(Map<String, Object> params) {
        if(this.params == null){
            this.params = new java.util.LinkedHashMap<>();
        }
        this.params.putAll(params);
    }

}
