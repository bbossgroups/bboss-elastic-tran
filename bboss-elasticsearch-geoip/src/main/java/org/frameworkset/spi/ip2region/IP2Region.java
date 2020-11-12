package org.frameworkset.spi.ip2region;
/**
 * Copyright 2020 bboss
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


import com.frameworkset.util.DaemonThread;
import com.frameworkset.util.ResourceInitial;
import org.frameworkset.spi.BaseApplicationContext;
import org.frameworkset.spi.geoip.IpInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/11/12 16:28
 * @author biaoping.yin
 * @version 1.0
 */
public class IP2Region {
	private static Logger logger = LoggerFactory.getLogger(IP2Region.class);
	private DbSearcher searcher;
	private DaemonThread daemonThread ;
	private String ip2regionDatabase;
	public  void init(String ip2regionDatabase){
		if(searcher != null){
			return;
		}
		synchronized (this) {
			if(searcher == null) {
				try {
					this.ip2regionDatabase = ip2regionDatabase;
					DbConfig config = new DbConfig();
					DbSearcher searcher = new DbSearcher(config, ip2regionDatabase);
					this.searcher = searcher;
				} catch (Exception e) {
					if (logger.isErrorEnabled())
						logger.error(ip2regionDatabase, e);
					throw new IP2RegionException("Init ip2regionDatabase failed:"+ip2regionDatabase,e);
				}
			}
			daemonThread = new DaemonThread(5000,"ip2regionDatabase-Reload");

			daemonThread.addFile(new File(ip2regionDatabase), new ResourceInitial() {
				@Override
				public void reinit() {
					reinit();
				}
			});
			daemonThread.start();
			BaseApplicationContext.addShutdownHook(new Runnable() {
				@Override
				public void run() {
					daemonThread.stopped();
					closeDb();
				}
			});
		}
	}
	private void closeDb(){
		if(searcher != null){
			try {
				searcher.close();
			} catch (IOException e) {
				logger.debug("closeDb failed:",e);
			}
			searcher = null;
		}
	}
	private synchronized void reinit(){
		DbSearcher oldSearcher = searcher;
		try {
			DbConfig config = new DbConfig();
			DbSearcher searcher = new DbSearcher(config, ip2regionDatabase);
			this.searcher = searcher;
			if(oldSearcher != null)
				oldSearcher.close();
		} catch (Exception e) {
			if (logger.isErrorEnabled())
				logger.error(ip2regionDatabase, e);
		}
	}
	public void assertInit(){
		if(searcher == null)
			throw new IP2RegionException("searcher not inited.");
	}

	public IpInfo getAddressMapResult(String ip){
		assertInit();
		if ( Util.isIpAddress(ip) == false ){
			return null;
		}
		try {
			DataBlock dataBlock = searcher.memorySearch(ip);
			if(dataBlock == null)
				return null;
			String region = dataBlock.getRegion();
			String[] infos = region.split("\\|");
			if(infos.length != 5){
				return null;
			}
			IpInfo ipInfo = new IpInfo();
			ipInfo.setCountry(infos[0]);
			ipInfo.setArea(infos[1]);
			ipInfo.setRegion(infos[2]);
			ipInfo.setCity(infos[3]);
			ipInfo.setIsp(infos[4]);
			ipInfo.setCityId(dataBlock.getCityId()+"");
			return ipInfo;


		} catch (IOException e) {
			logger.error(ip,e);
			return null;
		}
	}
}
