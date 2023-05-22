package org.frameworkset.spi.ip2regionv2.xdb;
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
import org.frameworkset.spi.ip2region.IP2Region;
import org.frameworkset.spi.ip2region.IP2RegionException;
import org.frameworkset.spi.ip2region.Util;
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
public class IP2RegionV2 implements IP2Region {
	private static final Logger logger = LoggerFactory.getLogger(IP2RegionV2.class);
	private Searcher searcher;
	private DaemonThread daemonThread ;
	private String ip2regionDatabase;
	@Override
	public  void init(String ip2regionDatabase,boolean enableBtree){
		if(searcher != null){
			return;
		}
		synchronized (this) {
			if(searcher == null) {
				try {
					this.ip2regionDatabase = ip2regionDatabase;
					byte[] cBuff = Searcher.loadContentFromFile(ip2regionDatabase);
					// 2、使用上述的 cBuff 创建一个完全基于内存的查询对象。
					Searcher searcher = Searcher.newWithBuffer(cBuff);
					this.searcher = searcher;

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



				} catch (Exception e) {
					if (logger.isErrorEnabled())
						logger.error("Init ip2regionDatabase failed:"+ip2regionDatabase, e);

					throw new IP2RegionException("Init ip2regionDatabase failed:"+ip2regionDatabase,e);
				}
			}

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
		final Searcher oldSearcher = searcher;
		try {
			byte[] cBuff = Searcher.loadContentFromFile(ip2regionDatabase);
			// 2、使用上述的 cBuff 创建一个完全基于内存的查询对象。
			Searcher searcher = Searcher.newWithBuffer(cBuff);
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
			String region = searcher.search(ip);
			if(region == null)
				return null;
			String[] infos = region.split("\\|");
			if(infos.length != 5){
				return null;
			}
			//`国家|区域|省份|城市|ISP`
			IpInfo ipInfo = new IpInfo();
			ipInfo.setCountry(infos[0]);
			if(!infos[1].equals( "0"))
				ipInfo.setArea(infos[1]);
			if(!infos[2].equals( "0"))
				ipInfo.setRegion(infos[2]);
			if(!infos[3].equals( "0"))
				ipInfo.setCity(infos[3]);
			if(!infos[4].equals( "0"))
				ipInfo.setIsp(infos[4]);
//			ipInfo.setCityId(dataBlock.getCityId()+"");
			ipInfo.setIp(ip);
			return ipInfo;


		} catch (IOException e) {
			logger.error(ip,e);
			return null;
		} catch (Exception e) {
			logger.error(ip,e);
			return null;
		}
	}
	@Override
	public IpInfo getAddressMapResult(String ip){
		return getIpInfo(ip);
	}
}
