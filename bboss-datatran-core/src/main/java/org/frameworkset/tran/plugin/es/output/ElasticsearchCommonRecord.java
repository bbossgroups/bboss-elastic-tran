package org.frameworkset.tran.plugin.es.output;
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

import com.frameworkset.orm.annotation.ESIndexWrapper;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.plugin.db.output.JDBCGetVariableValue;
import org.frameworkset.tran.plugin.es.ESField;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/5/10
 */
public class ElasticsearchCommonRecord extends CommonRecord {
    private Object esId;
    private Object parentId;
    private Object routing ;
    private ClientOptions clientOptions;
    private ESIndexWrapper esIndexWrapper;
    private JDBCGetVariableValue jdbcGetVariableValue;
    private String operation;

    public Object getEsId() {
        return esId;
    }
    public Object getVersion() throws Exception {
        ClientOptions clientOptions = getClientOptions();
        ESField esField = clientOptions != null ?clientOptions.getVersionField():null;
        Object version = null;
        if(esField != null) {
            if(!esField.isMeta())
                version = getData(esField.getField());
            else{
                version = getMetaValue(esField.getField());
            }
        }
        else {
            version =  clientOptions != null?clientOptions.getVersion():null;
        }
        return version;
    }
    public void setEsId(Object esId) {
        this.esId = esId;
    }

    public Object getParentId() {
        return parentId;
    }

    public void setParentId(Object parentId) {
        this.parentId = parentId;
    }

    public Object getRouting() {
        return routing;
    }

    public void setRouting(Object routing) {
        this.routing = routing;
    }

    public ClientOptions getClientOptions() {
        return clientOptions;
    }

    public void setClientOptions(ClientOptions clientOptions) {
        this.clientOptions = clientOptions;
    }

 

    public ESIndexWrapper getEsIndexWrapper() {
        return esIndexWrapper;
    }

    public void setEsIndexWrapper(ESIndexWrapper esIndexWrapper) {
        this.esIndexWrapper = esIndexWrapper;
    }

    public JDBCGetVariableValue getJdbcGetVariableValue() {
        return jdbcGetVariableValue;
    }

    public void setJdbcGetVariableValue(JDBCGetVariableValue jdbcGetVariableValue) {
        this.jdbcGetVariableValue = jdbcGetVariableValue;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }
}
