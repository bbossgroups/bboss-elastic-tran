package org.frameworkset.tran.plugin.feishu.input;
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.feishu.BaseFeishuTableConfig;
import org.slf4j.Logger;

import java.util.Map;

/**
 * @author biaoping.yin
 * @Date 2026/3/23
 */
public class FeishuTableInputConfig extends BaseFeishuTableConfig<FeishuTableInputConfig> implements InputConfig<FeishuTableInputConfig> {
    private static Logger logger = org.slf4j.LoggerFactory.getLogger(FeishuTableInputConfig.class);

   private String requestBody;
   @JsonIgnore
   private Map<String,FieldValueConvertor> fieldValueConvertors;
   @JsonIgnore
   private AllFieldValueConvertor allFieldValueConvertor;

    @Override
    public void build(ImportContext importContext, ImportBuilder importBuilder) {
        super.build(importContext,importBuilder);
        if(SimpleStringUtil.isEmpty(requestBody)){
            throw new DataImportException("requestBody can not be empty");
        }
        if(SimpleStringUtil.isEmpty(userIdType)){
            userIdType = "open_id";
        }
        Integer fetchSize = importContext.getFetchSize();
        if(fetchSize == null){
            fetchSize = 500;
        }
        else if (fetchSize > 500){
            logger.info("fetchSize={} can not be greater than 500, so set fetchSize to 500", fetchSize);
            fetchSize = 500;
            
        }
        searchUrl = "/open-apis/bitable/v1/apps/"
                +super.feishuTableAppToken+"/tables/"
                +super.feishuTableId+"/records/search?page_size="
                +fetchSize+"&user_id_type="+userIdType   ;
    }

   

    @Override
    public InputPlugin getInputPlugin(ImportContext importContext) {
        return new FeishuInputDataTranPlugin(importContext);
    }

    public String getRequestBody() {
        return requestBody;
    }

    public FeishuTableInputConfig setRequestBody(String requestBody) {
        this.requestBody = requestBody;
        return this;
    }


    public FeishuTableInputConfig registFieldValueConvertor(String field,FieldValueConvertor fieldValueConvertor) {
        if(fieldValueConvertors == null){
            fieldValueConvertors = new java.util.HashMap<>();
        }
        fieldValueConvertors.put(field,fieldValueConvertor);
        return this;
    }

    public FieldValueConvertor fieldValueConvertor(String field) {
        if(fieldValueConvertors != null){
            return fieldValueConvertors.get(field);
        }
        return null;
    }
    
    public FeishuTableInputConfig setAllFieldValueConvertor(AllFieldValueConvertor allFieldValueConvertor) {
        this.allFieldValueConvertor = allFieldValueConvertor;
        return this;
    }
    
    public AllFieldValueConvertor getAllFieldValueConvertor() {
        return allFieldValueConvertor;
    }
}
