package org.frameworkset.tran.script;


import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyShell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 *
 * @author biaoping.yin
 * @version 1.0
 * @Date 2022/12/6 14:50
 */
public class GroovyUtil {
    private static final Logger log = LoggerFactory.getLogger(GroovyUtil.class);
 
    public static Object evaluate( Binding binding,String script) throws GroovyEvalException {
		GroovyShell groovyShell = null;
		try {
			groovyShell = new GroovyShell(binding);
			Object value = groovyShell.evaluate(script);
			return value;
        } catch (Exception evalError) {

            throw new GroovyEvalException(script,evalError);
        }
		finally {
			groovyShell.resetLoadedClasses();
			groovyShell = null;
		}
    }
	public static void initImport(StringBuilder sb){
//        StringBuilder sb = new StringBuilder();
		sb.append("import org.frameworkset.spi.remote.http.HttpRequestProxy;\n");
		sb.append("import org.apache.commons.collections.*;\n");
		sb.append("import org.apache.commons.lang3.*;\n");
		sb.append("import java.text.*;\n");
		sb.append("import com.ai.visualops.entity.datamining.*;\n");
		sb.append("import com.frameworkset.util.*;\n");
		
		sb.append("import java.util.*;\n");
		sb.append("import com.ai.visualops.util.IndexUtil;\n");
		sb.append("import org.frameworkset.tran.metrics.job.MetricsConfig;\n");
		sb.append("import org.frameworkset.tran.plugin.db.input.DBRecordBuilderContext;\n");
		sb.append("import org.frameworkset.tran.CommonRecord;\n");
		sb.append("import org.frameworkset.tran.metrics.entity.*;\n");
		
		sb.append("import java.sql.ResultSet;\n");
	}
	
	
	
	
	/**
	 * 作业中使用基于groovy和java语法的低代码脚本，如果需要在脚本中导入其他类，必须使用[import][/import]标签在脚本开头导入需要使用的类，例如：
	 *             [import]
	 *             import java.sql.SQLException;
	 *             import java.util.Date;
	 *             [/import]
	 *             .......java或者groovy脚本
	 *
	 *             脚本默认导入的包类清单：
	 *             		import org.frameworkset.spi.remote.http.HttpRequestProxy;
	 *                     import org.apache.commons.collections.*;
	 *                     import org.apache.commons.lang3.*;
	 *                     import java.text.*;
	 *                     import com.ai.visualops.entity.datamining.*;
	 *                     import com.frameworkset.util.*;
	 *
	 *                     import java.util.*;
	 *                     import com.ai.visualops.util.IndexUtil;
	 *                     import org.frameworkset.tran.metrics.job.MetricsConfig;
	 *                     import org.frameworkset.tran.plugin.db.input.DBRecordBuilderContext;
	 *                     import org.frameworkset.tran.CommonRecord;
	 *                     import org.frameworkset.tran.metrics.entity.*;                    
	 *                     import java.sql.ResultSet;
	 * @param code
	 * @return
	 */
	
	public static CodeStruction parseCode(String code){
		CodeStruction codeStruction = new CodeStruction();
		if(code == null || code.equals("")){
			codeStruction.setCode( "");
			return codeStruction;
		}
		code = code.trim();
		String imports = null;
		
		if(code.startsWith("[import]")){
			StringBuilder builder = new StringBuilder();
			int start = "[import]".length();
			if( code.length() > start ){
				
				do{
					char c = code.charAt(start);
					if(c != '['){
						builder.append(code.charAt(start));
						start ++;
					}
					else{
						start = start + "[/import]".length();
						if(start > code.length())
							throw new GroovyScriptException("code import define error，import包的格式为[import]import类清单[/import]，例如：[import]import java.sql.SQLException;import java.util.Date;[/import]\r\n"+code);
						code = code.substring(start);
						imports = builder.toString();
						break;
					}
					if(start > code.length())
						throw new GroovyScriptException("code import define error，import包的格式为[import]import类清单[/import]，例如：[import]import java.sql.SQLException;import java.util.Date;[/import]\r\n"+code);
					
				}while (true);
				codeStruction.setImports(imports);
			}
			
			
			
		}
		codeStruction.setCode(code);
		return codeStruction;
	}
	
	public static void main(String[] args){
		
	}
	
	public static <T> T defineAPI(GroovyClassLoader groovyClassLoader, String apiCode, Class<T> apiType) {
		try {
			Class filenameGeneratorAPIClass = groovyClassLoader.parseClass(apiCode);
			log.info("DefineAPI impl class:"+filenameGeneratorAPIClass.getCanonicalName());
			T api = (T)filenameGeneratorAPIClass.newInstance();
			return api;
		}
		catch (Exception exception){
			throw new GroovyScriptException("Define "+apiType.getCanonicalName()+" failed:"+apiCode,exception);
		}
	}
	
	
}
