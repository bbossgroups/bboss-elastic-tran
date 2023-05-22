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
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.util.shutdown.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/11/12 16:28
 * @author yinbp<yin-bp@163.com>
 * @version 1.0
 */
public class IP2RegionV1  implements IP2Region{
	private static final Logger logger = LoggerFactory.getLogger(IP2RegionV1.class);
	private DbSearcher searcher;
	private DaemonThread daemonThread ;
	private String ip2regionDatabase;
	private boolean enableBtree;
	@Override
	public  void init(String ip2regionDatabase,boolean enableBtree){
		if(searcher != null){
			return;
		}
		this.enableBtree = enableBtree;
		synchronized (this) {
			if(searcher == null) {
				try {
					this.ip2regionDatabase = ip2regionDatabase;
					DbConfig config = new DbConfig();
					DbSearcher searcher = new DbSearcher(config, ip2regionDatabase,enableBtree);
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
					_reinit();
				}
			});
			daemonThread.start();
			ShutdownUtil.addShutdownHook(new Runnable() {
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
	private synchronized void _reinit(){
		final DbSearcher oldSearcher = searcher;
		try {
			DbConfig config = new DbConfig();
			DbSearcher searcher = new DbSearcher(config, ip2regionDatabase,  enableBtree);
			this.searcher = searcher;

			Thread t = new Thread(){
				@Override
				public void run() {
					synchronized (this){
						try {
							sleep(60000l);//延迟60秒关闭老对象

						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					if(oldSearcher != null) {
						try {
							logger.info("Delay 60s and close old ip2region searcher database.");
							oldSearcher.close();
						}
						catch (Exception e){
							if (logger.isErrorEnabled())
								logger.error("Reinit ip2region searcher database "+ip2regionDatabase + " failed:", e);
						}
					}

				}
			};
			t.start();

		} catch (Exception e) {
			if (logger.isErrorEnabled())
				logger.error("Reinit ip2region searcher database "+ip2regionDatabase + " failed:", e);
		}
	}
	private void assertInit(){
		if(searcher == null)
			throw new IP2RegionException("ip2region searcher database "+ip2regionDatabase + " not inited.");
	}
	@Override
	public IpInfo getIpInfo(String ip){
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
			ipInfo.setIp(ip);
			return ipInfo;


		} catch (IOException e) {
			logger.error(ip,e);
			return null;
		}
	}
	@Override
	public IpInfo getAddressMapResult(String ip){
		return getIpInfo(ip);
	}
}
