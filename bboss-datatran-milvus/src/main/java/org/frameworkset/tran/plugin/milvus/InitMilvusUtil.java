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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.nosql.milvus.MilvusConfig;
import org.frameworkset.nosql.milvus.MilvusHelper;
import org.frameworkset.nosql.milvus.MilvusStartResult;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/11/29
 */
public abstract class InitMilvusUtil {
    public static MilvusStartResult initMilvus(MilvusConfigInf milvusConfigInf){
        MilvusConfig milvusConfig = new MilvusConfig();
        milvusConfig.setName(milvusConfigInf.getName());
        if(SimpleStringUtil.isNotEmpty(milvusConfigInf.getUri())) {
            
            milvusConfig.setUri(milvusConfigInf.getUri());
            milvusConfig.setToken(milvusConfigInf.getToken());
            milvusConfig.setMaxIdlePerKey(milvusConfigInf.getMaxIdlePerKey());
            milvusConfig.setMinIdlePerKey(milvusConfigInf.getMinIdlePerKey());
            milvusConfig.setMaxTotalPerKey(milvusConfigInf.getMaxTotalPerKey());
            milvusConfig.setMaxTotal(milvusConfigInf.getMaxTotal());
            milvusConfig.setBlockWhenExhausted(milvusConfigInf.getBlockWhenExhausted());
            milvusConfig.setMaxBlockWaitDuration(milvusConfigInf.getMaxBlockWaitDuration());
            milvusConfig.setMinEvictableIdleDuration(milvusConfigInf.getMinEvictableIdleDuration());
            milvusConfig.setEvictionPollingInterval(milvusConfigInf.getEvictionPollingInterval());
            milvusConfig.setTestOnBorrow(milvusConfigInf.getTestOnBorrow());
            milvusConfig.setTestOnReturn(milvusConfigInf.getTestOnReturn());
            milvusConfig.setDbName(milvusConfigInf.getDbName());

            milvusConfig.setConnectTimeoutMs(milvusConfigInf.getConnectTimeoutMs());
            milvusConfig.setIdleTimeoutMs(milvusConfigInf.getIdleTimeoutMs());
            milvusConfig.setCustomConnectConfigBuilder(milvusConfigInf.getCustomConnectConfigBuilder());


            MilvusStartResult milvusStartResult = MilvusHelper.init(milvusConfig);
            return milvusStartResult;
        }
        return null;
        
    }
}
