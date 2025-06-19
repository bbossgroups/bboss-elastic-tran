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

import org.frameworkset.tran.jobflow.JobFlowStatus;

/**
 * @author biaoping.yin
 * @Date 2025/6/19
 */
public class AssertResult {
    private JobFlowStatus jobFlowStatus;
    private boolean result;
    public AssertResult(JobFlowStatus jobFlowStatus,boolean result){
        this.jobFlowStatus = jobFlowStatus;
        this.result = result;
    }

    public JobFlowStatus getJobFlowStatus() {
        return jobFlowStatus;
    }

    public boolean isTrue() {
        return result == true;
    }

    public boolean isFalse() {
        return result == false;
    }
}
