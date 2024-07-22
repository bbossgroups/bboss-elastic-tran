package org.frameworkset.nosql.hbase;
/**
 * Copyright 2008 biaoping.yin
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

import java.util.concurrent.ConcurrentHashMap;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.plugin.hbase.HBasePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2020/1/4 17:50
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseHelperFactory {
	private static Logger logger = LoggerFactory.getLogger(HBaseHelperFactory.class);
	private static Map<String,HBaseHelper> hBaseHelperMap = new ConcurrentHashMap();
	public static HBaseHelper getHBaseHelper(String name){
		return hBaseHelperMap.get(name);
	}
	public static void destroy(String name){
		synchronized (hBaseHelperMap) {
			HBaseHelper hBaseHelper = getHBaseHelper(name);
			if (hBaseHelper != null) {
				hBaseHelper.destroy();
				hBaseHelperMap.remove(name);
			}
		}
	}
	public static  boolean buildHBaseClient(HBasePluginConfig hBasePluginConfig){
		if(SimpleStringUtil.isNotEmpty(hBasePluginConfig.getHbaseClientProperties())){
			HBaseHelper hBaseHelper  = hBaseHelperMap.get(hBasePluginConfig.getName());
			if(hBaseHelper == null){
				synchronized (hBaseHelperMap){
					hBaseHelper  = hBaseHelperMap.get(hBasePluginConfig.getName());
					if(hBaseHelper == null){
						hBaseHelper = new HBaseHelper();
						hBaseHelper.buildHBaseClient(hBasePluginConfig.getHbaseClientProperties(),
								hBasePluginConfig.getHbaseClientThreadCount(),
								hBasePluginConfig.getHbaseClientThreadQueue(),
								hBasePluginConfig.getHbaseClientKeepAliveTime(),
								hBasePluginConfig.getHbaseClientBlockedWaitTimeout(),
								hBasePluginConfig.getHbaseClientWarnMultsRejects(),
								hBasePluginConfig.isHbaseClientPreStartAllCoreThreads(),
								hBasePluginConfig.getHbaseClientThreadDaemon());
						hBaseHelperMap.put(hBasePluginConfig.getName(),hBaseHelper);
						return true;
					}
				}
			}
		}

		return false;

	}

}
