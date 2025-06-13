package org.frameworkset.tran.script;
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

import groovy.lang.GroovyClassLoader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author biaoping.yin
 * @Date 2025/6/13
 */
public class GroovyUtilTest {
    private static final Logger logger = LoggerFactory.getLogger(GroovyUtilTest.class);
    @Test
    public void test(){
        try {
            GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
            StringBuilder sb = new StringBuilder();

            sb.append("import com.ai.visualops.entity.funnel.FunnelScriptException;\n");
            sb.append("import org.slf4j.Logger;\n");
            sb.append("import com.ai.visualops.metrics.event.BaseEventCode;" );
            sb.append("import com.ai.visualops.entity.funnel.EventRuleAPI;" );

            String apiName =  "EventRuleAPIImpl";
            sb.append("public class " + apiName+ " implements EventRuleAPI {\r\n");
            sb.append("public boolean api(Logger logger, BaseEventCode _event, Map<String, Object> eventLabels,Map eventVT) \n" +
                            "\t\t\tthrows FunnelScriptException {\r\n")
                    .append("return true;").append("\r\n}\r\n}");
            Class clazz = groovyClassLoader.parseClass(sb.toString());
            sb = new StringBuilder();

            sb.append("import com.ai.visualops.entity.funnel.FunnelScriptException;\n");
            sb.append("import org.slf4j.Logger;\n");
            sb.append("import com.ai.visualops.metrics.event.BaseEventCode;" );
            sb.append("import com.ai.visualops.entity.funnel.EventRuleAPI;" );

            sb.append("public class " + apiName+ " implements EventRuleAPI {\r\n");
            sb.append("public boolean api(Logger logger, BaseEventCode _event, Map<String, Object> eventLabels,Map eventVT) \n" +
                            "\t\t\tthrows FunnelScriptException {\r\n")
                    .append("return false;").append("\r\n}\r\n}");
            Class clazz1 = groovyClassLoader.parseClass(sb.toString());
            EventRuleAPI obj = (EventRuleAPI)clazz.newInstance();
            EventRuleAPI obj1 = (EventRuleAPI)clazz1.newInstance();
            boolean r1 = obj.api(null,null,null,null);
            boolean r2 = obj1.api(null,null,null,null);
            groovyClassLoader.close();
            obj = (EventRuleAPI)clazz.newInstance();

            obj1 = (EventRuleAPI)clazz1.newInstance();
            r1 = obj.api(null,null,null,null);
            r2 = obj1.api(null,null,null,null);


            logger.info("ok");
//			Binding binding = new Binding();
//			binding.setProperty("bulkDataList", "ddddd");
//			String script = " String a = 1;" +
//					"println(bulkDataList);" +
//					"	[a,bulkDataList]";
//			
//			GroovyShell groovyShell = new GroovyShell(binding);
//			Object warnMsg = GroovyUtil.evaluate(groovyShell,script);
//			log.info(warnMsg+"");

        }

        catch (Exception evalError) {
            logger.error("",evalError);
        }
    }

}
