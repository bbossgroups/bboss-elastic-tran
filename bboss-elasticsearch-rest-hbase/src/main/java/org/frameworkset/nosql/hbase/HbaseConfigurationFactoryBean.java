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

package org.frameworkset.nosql.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Enumeration;
import java.util.Properties;

/**
 * Factory for creating HBase specific configuration. By default cleans up any connection associated with the current configuration.
 *
 * @author Costin Leau
 */
public class HbaseConfigurationFactoryBean{

    private Configuration configuration;
    private Configuration hadoopConfig;
    private Properties properties;

    /**
     * Sets the Hadoop configuration to use.
     *
     * @param configuration The configuration to set.
     */
    public void setConfiguration(Configuration configuration) {
        this.hadoopConfig = configuration;
    }

    /**
     * Sets the configuration properties.
     *
     * @param properties The properties to set.
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void afterPropertiesSet() {
        configuration = (hadoopConfig != null ? HBaseConfiguration.create(hadoopConfig) : HBaseConfiguration.create());
        addProperties(configuration, properties);


//        //刷新kgt
//        try {
//
//          /*  configuration.set("hadoop.security.authentication", "Kerberos");
//            configuration.set("keytab.file", "/app/apm/consumer/config/user.keytab");
//            configuration.set("kerberos.principal", "yxrongh@HADOOP.COM");*/
//
//            String user = configuration.get("kerberos.principal");
//            String path = configuration.get("keytab.file");
//            long interval = Long.parseLong(configuration.get("kerberos.refresh.interval"));
//            System.out.println("333333333>>> user:"+ user+",path:"+path+",interval:"+interval);
//            UserGroupInformation.setConfiguration(configuration);
//
//            UserGroupInformation.loginUserFromKeytab(user, path);
//
//            HbaseKerberosLogin login = new HbaseKerberosLogin();
//            login.autoRefreshThreadForKbTgt(interval);
//        }catch(Exception e){
//            System.out.println("刷新tgt失败");
//            e.printStackTrace();
//        }

    }
    
    /**
     * Adds the specified properties to the given {@link Configuration} object.
     * 
     * @param configuration configuration to manipulate. Should not be null.
     * @param properties properties to add to the configuration. May be null.
     */
    private void addProperties(Configuration configuration, Properties properties) {
//        Assert.notNull(configuration, "A non-null configuration is required");
        if (properties != null) {
            Enumeration<?> props = properties.propertyNames();
            while (props.hasMoreElements()) {
                String key = props.nextElement().toString();
                configuration.set(key, properties.getProperty(key));
            }
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }



}