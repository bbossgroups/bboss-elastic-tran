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
import org.frameworkset.spi.feishu.FeishuHelper;
import org.frameworkset.spi.remote.http.HttpConfigInf;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.feishu.BaseFeishuTableConfig;
import org.slf4j.Logger;

/**
 * @author biaoping.yin
 * @Date 2026/3/23
 */
public class FeishuTableInputConfig extends BaseFeishuTableConfig<FeishuTableInputConfig> implements InputConfig<FeishuTableInputConfig>, HttpConfigInf {
    private static Logger logger = org.slf4j.LoggerFactory.getLogger(FeishuTableInputConfig.class);

   private String requestBody;

   

    protected boolean showDsl;
    /**
     * 通过虚拟一个自定义dsl管理容器，实现queryDsl的模拟配置文件加载,保持接口逻辑的统一管理
     */
    protected String dslNamespace;
    protected String dslFile;
    private String queryDslName;
 
   @JsonIgnore
   private FieldValueConvertor fieldValueConvertor;

    @Override
    public void build(ImportContext importContext, ImportBuilder importBuilder) {
        super.build(importContext,importBuilder);
        if(SimpleStringUtil.isEmpty(requestBody) && SimpleStringUtil.isEmpty(dslFile)   ) {
            throw new DataImportException("requestBody can not be empty");
        }
        if(SimpleStringUtil.isEmpty(queryDslName))
            queryDslName = "feishuQueryDslName";
        if(SimpleStringUtil.isEmpty(dslNamespace))
            dslNamespace = "feishuQueryDslNamespace"+SimpleStringUtil.getUUID32();
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
        searchUrl = FeishuHelper.buildSearchUrl( super.feishuTableAppToken,super.feishuTableId,fetchSize,userIdType  ) ;
        if(fieldValueConvertor == null){
            fieldValueConvertor = new FieldValueConvertor();
        }
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


    public FieldValueConvertor getFieldValueConvertor() {
        return fieldValueConvertor;
    }

    public FeishuTableInputConfig setFieldValueConvertor(FieldValueConvertor fieldValueConvertor) {
        this.fieldValueConvertor = fieldValueConvertor;
        return this;
    }

    public String getQueryDslName() {
        return queryDslName;
    }

    public FeishuTableInputConfig setQueryDslName(String queryDslName) {
        this.queryDslName = queryDslName; 
        return this;
    }
       

    public boolean isShowDsl() {
        return showDsl;
    }

    public FeishuTableInputConfig setShowDsl(boolean showDsl) {
        this.showDsl = showDsl;
        return this;
    }

    public String getDslNamespace() {
        return dslNamespace;
    }

    public FeishuTableInputConfig setDslNamespace(String dslNamespace) {
        this.dslNamespace = dslNamespace;
        return this;
    }

    public String getDslFile() {
        return dslFile;
    }

    public FeishuTableInputConfig setDslFile(String dslFile) {
        this.dslFile = dslFile;
        return this;
    }
}
