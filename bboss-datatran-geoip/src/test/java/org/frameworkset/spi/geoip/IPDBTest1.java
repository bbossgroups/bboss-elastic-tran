package org.frameworkset.spi.geoip;
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

import com.frameworkset.util.SimpleStringUtil;

import java.io.IOException;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/11/12 21:43
 * @author biaoping.yin
 * @version 1.0
 */
public class IPDBTest1 {
	public static void main(String[] args)  throws IOException{
		test() ;
	}
//	@Test
	public static void test() throws IOException {



		// 测试ip 221.232.245.73 湖北武汉
		try {
			GeoIPUtil addressUtils = new GeoIPUtil();
			addressUtils.setAsnDatabase("C:\\data\\geolite2\\GeoLite2-ASN.mmdb");
			addressUtils.setDatabase("C:\\data\\geolite2\\GeoLite2-City.mmdb");
			addressUtils.setIp2regionDatabase("C:\\data\\geolite2\\ip2region_v4.xdb;C:\\data\\geolite2\\ip2region_v6.xdb");
			addressUtils.setCachesize(2000);
			addressUtils.init();
			IpInfo address = addressUtils.getIpInfo("1111:8922:8e10:322:c558:432b:1111:6d5c");
			System.out.println(SimpleStringUtil.object2json(address));
            address = addressUtils.getIpInfo("2409:8922:8e10:322:c558:432b:5a65:6d5c");
            System.out.println(SimpleStringUtil.object2json(address));
            address = addressUtils.getIpInfo("223.104.130.11");
            System.out.println(SimpleStringUtil.object2json(address));
			address = addressUtils.getIpInfo("2409:8950:5ee1:d5c4:a5ce:69f0:d9fb:72c8");//ipv6
			System.out.println(SimpleStringUtil.object2json(address));
			address = addressUtils.getIpInfo("2409:8922:8e10:322:c558:432b:5a65:6d5c");//ipv6
			System.out.println(SimpleStringUtil.object2json(address));
            address = addressUtils.getIpInfo("101.90.2.179");
            System.out.println(SimpleStringUtil.object2json(address));
            address = addressUtils.getIpInfo("39.144.38.4");//ipv6
            System.out.println(SimpleStringUtil.object2json(address));
            address = addressUtils.getIpInfo("113.90.117.151");//ipv6
            System.out.println(SimpleStringUtil.object2json(address));
            
            //释放资源
            addressUtils.stop();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
