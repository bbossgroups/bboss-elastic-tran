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
				addressUtils.setAsnDatabase("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-ASN.mmdb");
				addressUtils.setDatabase("E:\\workspace\\hnai\\terminal\\geolite2\\GeoLite2-City.mmdb");
				addressUtils.setIp2regionDatabase("E:\\workspace\\hnai\\terminal\\geolite2\\ip2region.db");
				addressUtils.setCachesize(2000);
				addressUtils.init();
				IpInfo address = addressUtils.getAddressMapResult("223.104.130.11");
				System.out.println(address);
				address = addressUtils.getAddressMapResult("2409:8950:5ee1:d5c4:a5ce:69f0:d9fb:72c8");//ipv6
				System.out.println(address);


			} catch (Exception e) {
				e.printStackTrace();
			}

	}
}
