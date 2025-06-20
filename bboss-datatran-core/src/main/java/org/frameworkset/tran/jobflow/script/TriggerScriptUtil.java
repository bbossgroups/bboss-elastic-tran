package org.frameworkset.tran.jobflow.script;
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

import com.frameworkset.util.SimpleStringUtil;
import groovy.lang.GroovyClassLoader;
import org.apache.commons.lang3.StringUtils;
import org.frameworkset.tran.jobflow.JobFlow;
import org.frameworkset.tran.jobflow.NodeTrigger;
import org.frameworkset.tran.script.CodeStruction;
import org.frameworkset.tran.script.GroovyScriptException;
import org.frameworkset.tran.script.GroovyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 根据工作流节点触发器脚本
 * @author biaoping.yin
 * @Date 2025/6/13
 */
public class TriggerScriptUtil {
    private static Logger logger = LoggerFactory.getLogger(TriggerScriptUtil.class);
   
    public static TriggerScriptAPI evalTriggerScript(JobFlow jobFlow,NodeTrigger nodeTrigger) {
        String script = null;
        try {
            GroovyClassLoader groovyClassLoader = jobFlow.getGroovyClassLoader();
            CodeStruction codeStruction = GroovyUtil.parseCode(nodeTrigger.getTriggerScript());
            StringBuilder code = new StringBuilder();
            if (StringUtils.isNotEmpty(codeStruction.getImports())) {
                code.append(codeStruction.getImports());
            }
            code.append("import org.frameworkset.tran.jobflow.JobFlow;\n");
            code.append("import org.frameworkset.tran.jobflow.JobFlowNode;\n");
            code.append("import org.frameworkset.tran.jobflow.context.NodeTriggerContext;\n");
            
            code.append("import org.frameworkset.tran.jobflow.context.JobFlowExecuteContext;\n");
            code.append("import org.frameworkset.tran.jobflow.script.TriggerScriptAPI;\n");
            code.append("\n");
            String apiName = "TriggerScriptAPIImpl_" +SimpleStringUtil.getUUID32() ;

            code.append("public class " + apiName + " implements TriggerScriptAPI {\r\n");
            code.append("boolean evalTriggerScript(NodeTriggerContext nodeTriggerContext) throws Exception {\r\n")
                    .append(codeStruction.getCode()).append("\r\n}\r\n}");
            script = code.toString();
            Class clazz = groovyClassLoader.parseClass(script);
            if(logger.isDebugEnabled())
                logger.debug("apiName {},evalTriggerScript: {} ",apiName,nodeTrigger.getTriggerScript());
            return (TriggerScriptAPI) clazz.getDeclaredConstructor().newInstance();
        }
        catch (Exception e){
            throw new GroovyScriptException(script,e);
        }
    }
}
