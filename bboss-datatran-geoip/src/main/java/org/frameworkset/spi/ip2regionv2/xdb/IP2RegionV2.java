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
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.spi.ip2region.IP2Region;
import org.frameworkset.spi.ip2region.IP2RegionException;
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
	private Searcher searcher_ipv4;

    private Searcher searcher_ipv6;
	private DaemonThread daemonThread ;
    
	private String ip2regionDatabaseIPV4;
    private String ip2regionDatabaseIPV6;
    private Object lock = new Object();
    
    
    private void startMonitor(){
        if(daemonThread == null) {
            daemonThread = new DaemonThread(5000, "ip2regionDatabaseIP-Reload");
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
	@Override
	public  void init(String[] ip2regionDatabases,boolean enableBtree){
		if(searcher_ipv4 != null){
			return;
		}
		synchronized (lock) {
			if(searcher_ipv4 == null) {
				try {
					this.ip2regionDatabaseIPV4 = ip2regionDatabases[0];
                    File ip2regionDatabaseIPV4File = new File(ip2regionDatabaseIPV4);
                    if(!ip2regionDatabaseIPV4File.exists()){
                        logger.warn("ip2regionDatabaseIPV4 file:{} not exists.", ip2regionDatabaseIPV4);
                        startMonitor();
                        daemonThread.addFile(new File(ip2regionDatabaseIPV4), new ResourceInitial() {
                            @Override
                            public void reinit() {
                                synchronized (lock) {
                                    searcher_ipv4 = _reinitIp(searcher_ipv4, Version.IPv4, ip2regionDatabaseIPV4);
                                }
                            }
                        },true);                         
                    }
                    else {
                        logger.info("Init ip2regionDatabaseIPV4:{}", ip2regionDatabaseIPV4);
                        LongByteArray cBuff = Searcher.loadContentFromFile(ip2regionDatabaseIPV4);
                        // 2、使用上述的 cBuff 创建一个完全基于内存的查询对象。
                        Searcher searcher = Searcher.newWithBuffer(Version.IPv4, cBuff);
                        this.searcher_ipv4 = searcher;

                        startMonitor();
                        daemonThread.addFile(new File(ip2regionDatabaseIPV4), new ResourceInitial() {
                            @Override
                            public void reinit() {
                                synchronized (lock) {
                                    searcher_ipv4 = _reinitIp(searcher_ipv4, Version.IPv4, ip2regionDatabaseIPV4);
                                }
                            }
                        }, true);
                    }
					



				} catch (Exception e) {
					if (logger.isErrorEnabled())
						logger.error("Init ip2regionDatabase failed:"+ ip2regionDatabaseIPV4, e);

					throw new IP2RegionException("Init ip2regionDatabase failed:"+ ip2regionDatabaseIPV4,e);
				}
			}
            if(searcher_ipv6 == null ){
                ip2regionDatabaseIPV6 = ip2regionDatabases[1];
                
                if(SimpleStringUtil.isNotEmpty(ip2regionDatabaseIPV6)) {
     
                    try {
                        File ip2regionDatabaseIPV6File = new File(ip2regionDatabaseIPV6);
                        if(!ip2regionDatabaseIPV6File.exists()){
                            logger.warn("ip2regionDatabaseIPV6 file:{} not exists.", ip2regionDatabaseIPV6);
                            startMonitor();

                            daemonThread.addFile(new File(ip2regionDatabaseIPV6), new ResourceInitial() {
                                @Override
                                public void reinit() {
                                    searcher_ipv6 = _reinitIp(searcher_ipv6,Version.IPv6,ip2regionDatabaseIPV6);
                                }
                            },true);
                           
                           
                        }
                        else {
                            logger.info("Init ip2regionDatabaseIPV6:{}", ip2regionDatabaseIPV6);
                            LongByteArray cBuff = Searcher.loadContentFromFile(ip2regionDatabaseIPV6);
                            // 2、使用上述的 cBuff 创建一个完全基于内存的查询对象。
                            Searcher searcher = Searcher.newWithBuffer(Version.IPv6, cBuff);
                            this.searcher_ipv6 = searcher;

                            startMonitor();

                            daemonThread.addFile(new File(ip2regionDatabaseIPV6), new ResourceInitial() {
                                @Override
                                public void reinit() {
                                    searcher_ipv6 = _reinitIp(searcher_ipv6, Version.IPv6, ip2regionDatabaseIPV6);
                                }
                            }, true);
                        }
                        
                    } catch (Exception e) {
                        if (logger.isErrorEnabled())
                            logger.error("Init ip2regionDatabase failed:" + ip2regionDatabaseIPV4, e);

                        throw new IP2RegionException("Init ip2regionDatabase failed:" + ip2regionDatabaseIPV4, e);
                    }
                }
            }

		}
	}
	private void closeDb(){
		if(searcher_ipv4 != null){
			try {
				searcher_ipv4.close();
			} catch (IOException e) {
				logger.debug("closeDb failed:",e);
			}
			searcher_ipv4 = null;

            
		}
        if(searcher_ipv6 != null){
            try {
                searcher_ipv6.close();
            } catch (IOException e) {
                logger.debug("closeDb failed:",e);
            }
            searcher_ipv6 = null;
        }
	}
	private  Searcher _reinitIp(Searcher oldSearcher,Version version,String dataFile){
//		final Searcher oldSearcher = searcher_ipv4;
		try {
            logger.info("Reinit ip2region searcher database:{},{}", dataFile,version.name);
			LongByteArray cBuff = Searcher.loadContentFromFile(dataFile);
			// 2、使用上述的 cBuff 创建一个完全基于内存的查询对象。
			Searcher searcher = Searcher.newWithBuffer(version,cBuff);
			

			Thread t = new Thread(){
				@Override
				public void run() {
					synchronized (this){
						try {
							sleep(60000l);//延迟60秒关闭老对象

						} catch (InterruptedException e) {
							logger.debug("Reinit ip2region searcher database "+ dataFile + " Interrupted:", e);
						}
					}
					if(oldSearcher != null) {
						try {
							logger.info("Delay 60s and close old ip2region searcher database.");
							oldSearcher.close();
						}
						catch (Exception e){
							if (logger.isErrorEnabled())
								logger.error("Reinit ip2region searcher database "+ dataFile + " failed:", e);
						}
					}

				}
			};
			t.start();
            return searcher;

		} catch (Exception e) {
			if (logger.isErrorEnabled())
				logger.error("Reinit ip2region searcher database "+ dataFile + " failed:", e);
            return oldSearcher;
		}
	}
	private void assertInit(){
		if(searcher_ipv4 == null && searcher_ipv6 == null)
			throw new IP2RegionException("ip2region searcher database "+ ip2regionDatabaseIPV4
                    + " and "+ ip2regionDatabaseIPV6+" not inited .");
	}
	@Override
	public IpInfo getIpInfo(String ip){
		assertInit();
        
		try {
            byte[] ipBytes = Util.parseIP(ip);
//            if ( Util.isIpAddress(ip) == false ){
//                return null;
//            }
            String region = null;
            if(Version.IPv4.bytes == ipBytes.length) {
                if(searcher_ipv4 != null)
                    region = searcher_ipv4.search(ip);
            }
            else 
                if(Version.IPv6.bytes == ipBytes.length) {
                    if(searcher_ipv6 != null)
                        region = searcher_ipv6.search(ip);
            }
            
			if(region == null)
				return null;
			String[] infos = region.split("\\|");
//			if(infos.length != 5){
//				return null;
//			}
            //`国家|区域|省份|城市|ISP`
            IpInfo ipInfo = new IpInfo();
            if(infos.length == 4){
                ipInfo.setCountry(infos[0]);
                 
                if(!infos[1].equals( "0"))
                    ipInfo.setRegion(infos[2]);
                if(!infos[2].equals( "0"))
                    ipInfo.setCity(infos[3]);
                if(!infos[3].equals( "0"))
                    ipInfo.setIsp(infos[3]);
            }
			else if(infos.length == 5) {
                ipInfo.setCountry(infos[0]);
                if (!infos[1].equals("0"))
                    ipInfo.setArea(infos[1]);
                if (!infos[2].equals("0"))
                    ipInfo.setRegion(infos[2]);
                if (!infos[3].equals("0"))
                    ipInfo.setCity(infos[3]);
                if (!infos[4].equals("0"))
                    ipInfo.setIsp(infos[4]);
            }
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
