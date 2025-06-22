package org.frameworkset.tran.script;
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

/**
 * <p>Description: groovy脚本import导入语法解析结构对象
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
 * </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/10/23
 */
public class CodeStruction {
	private String imports;
	private String code;
	
	public String getImports() {
		return imports;
	}
	
	public void setImports(String imports) {
		this.imports = imports;
	}
	
	public String getCode() {
		return code;
	}
	
	public void setCode(String code) {
		this.code = code;
	}
}
