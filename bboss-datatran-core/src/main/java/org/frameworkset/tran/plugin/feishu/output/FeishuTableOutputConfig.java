package org.frameworkset.tran.plugin.feishu.output;
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

import org.frameworkset.spi.feishu.FeishuHelper;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.feishu.BaseFeishuTableConfig;

/**
 * @author biaoping.yin
 * @Date 2026/3/23
 */
public class FeishuTableOutputConfig extends BaseFeishuTableConfig<FeishuTableOutputConfig> implements OutputConfig<FeishuTableOutputConfig> {
 
    private String batchInsertUrl;
    private String batchUpdateUrl;
    private String listFieldsUrl;

    private String batchDeleteUrl;
    
 

    @Override
    public void build(ImportContext importContext, ImportBuilder importBuilder) {
        super.build(importContext,importBuilder);
        if(this.cellMappingList == null || this.cellMappingList.size() == 0){
            throw new IllegalArgumentException("未配置飞书多维表格字段映射：cellMappingList is empty!");
        }
        batchInsertUrl = FeishuHelper.buildBatchInsertUrl(feishuTableAppToken,feishuTableId);
        batchUpdateUrl = FeishuHelper.buildBatchUpdateUrl(feishuTableAppToken,feishuTableId);
        batchDeleteUrl = FeishuHelper.buildBatchDeleteUrl(feishuTableAppToken,feishuTableId);
        listFieldsUrl = FeishuHelper.buildListFieldsUrl(feishuTableAppToken,feishuTableId);
        searchUrl = FeishuHelper.buildSearchUrl(feishuTableAppToken,feishuTableId,10,userIdType);
//        "/open-apis/bitable/v1/apps/"
//                +feishuTableAppToken+"/tables/"
//                +feishuTableId+"/records/search?page_size=10&user_id_type="+userIdType   ;
    }
 

    public String getBatchInsertUrl() {
        return batchInsertUrl;
    }

    public String getBatchUpdateUrl() {
        return batchUpdateUrl;
    }

    public String getBatchDeleteUrl() {
        return batchDeleteUrl;
    }

    public String getListFieldsUrl() {
        return listFieldsUrl;
    }

   
    /**
     * 根据上下文配置创建OutputPlugin
     *
     * @param importContext
     * @return
     */
    @Override
    public OutputPlugin getOutputPlugin(ImportContext importContext) {
        return new FeishuOutputDataTranPlugin(this,importContext);
    }
    
}
