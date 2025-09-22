package org.frameworkset.tran.plugin.milvus;
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

import org.frameworkset.nosql.milvus.CustomConnectConfigBuilder;


/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/11/29
 */
public interface MilvusConfigInf<T extends MilvusConfigInf> {
    public String getName() ;

    public String getCollectionName();


    public String getUri();


    public String getToken();


    public Integer getMaxIdlePerKey();


    public Integer getMinIdlePerKey();


    public Integer getMaxTotalPerKey();


    public Integer getMaxTotal() ;

    public Boolean getBlockWhenExhausted() ;
    public Long getMaxBlockWaitDuration() ;
    public Long getMinEvictableIdleDuration() ;
    public Long getEvictionPollingInterval();
    public Boolean getTestOnBorrow() ;
    public Boolean getTestOnReturn();
    public String getDbName() ;

    public Long getConnectTimeoutMs();

    public Long getIdleTimeoutMs() ;
    public CustomConnectConfigBuilder getCustomConnectConfigBuilder() ;


}
