package org.frameworkset.tran.jobflow.metrics;
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
 * @author biaoping.yin
 * @Date 2025/6/20
 */
public class JobFlowMetrics {
    private long totalCount;
    private long successCount ;
    private long exceptionCount;
    public long addTotalCount(){
        this.totalCount ++;
        return totalCount;
    }
    
    public void complete(Throwable e){
        if(e == null){
            addSuccessCount();
        }
        else{
            addExceptionCount();
        }
    }

    public long addSuccessCount(){
        this.successCount ++;
        return successCount;
    }

    public long addExceptionCount(){
        this.exceptionCount ++;
        return exceptionCount;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public long getSuccessCount() {
        return successCount;
    }

    public long getExceptionCount() {
        return exceptionCount;
    }
}
