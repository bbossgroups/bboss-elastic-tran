package org.frameworkset.tran.upgrade;
/**
 * Copyright 2023 bboss
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

import com.frameworkset.common.poolman.SQLExecutor;
import com.frameworkset.common.poolman.util.DBConf;
import com.frameworkset.common.poolman.util.SQLManager;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.status.LastValueWrapper;

import java.sql.SQLException;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/6/4
 * @author biaoping.yin
 * @version 1.0
 */
public class UpgradeBboss {
    public static void main(String[] args){
        UpgradeBboss upgradeBboss = new UpgradeBboss();
        DBConf tempConf = new DBConf();
        tempConf.setPoolname("upgrade");
        tempConf.setDriver("org.sqlite.JDBC");
        tempConf.setJdbcurl("jdbc:sqlite://E:\\workspace\\bbossgroups\\bboss-elastic\\StatusStoreDB");
        tempConf.setUsername("root");
        tempConf.setPassword("Root_123456#");
        tempConf.setReadOnly((String)null);
        tempConf.setTxIsolationLevel((String)null);
        tempConf.setValidationQuery("select 1");
        tempConf.setJndiName("upgrade-jndi");
        tempConf.setInitialConnections(1);
        tempConf.setMinimumSize(1);
        tempConf.setMaximumSize(1);
        tempConf.setUsepool(true);
        tempConf.setExternal(false);
        tempConf.setExternaljndiName((String)null);
        tempConf.setShowsql(false);
        tempConf.setEncryptdbinfo(false);
        tempConf.setQueryfetchsize(null);
        upgradeBboss.upgradeStatus(tempConf,"es2custom");
    }
    public void upgradeStatus(DBConf statusDBConf,String statusTable){
        boolean result = SQLManager.startPool(statusDBConf);
        String statusTableHis = statusTable + "_his";
        String sql = "select * from " + statusTable;
        String sqlHis = "select * from " + statusTableHis;
        String updateSql = "update "+ statusTable + " set strLastValue = ? where id=?";
        String updateSqlHis = "update "+ statusTableHis + " set strLastValue = ? where id=?";
        try {
            List<Status> statusList = SQLExecutor.queryListWithDBName( Status.class,statusDBConf.getPoolname(),sql);
            if(statusList != null && statusList.size() > 0) {
                for (Status status : statusList) {
                    LastValueWrapper lastValueWrapper = new LastValueWrapper();
//                    lastValueWrapper.setLastValue(status.getLastValue());
                    lastValueWrapper.setStrLastValue(status.getStrLastValue());
                    if(lastValueWrapper.getTimeStamp() == null)
                        lastValueWrapper.setTimeStamp(status.getTime());
                    status.setStrLastValue(SimpleStringUtil.object2json(lastValueWrapper));
                    SQLExecutor.updateWithDBName(statusDBConf.getPoolname(),updateSql,status.getStrLastValue(),status.getId());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try {
            List<Status> statusList = SQLExecutor.queryListWithDBName( Status.class,statusDBConf.getPoolname(),sqlHis);
            if(statusList != null && statusList.size() > 0) {
                for (Status status : statusList) {
                    LastValueWrapper lastValueWrapper = new LastValueWrapper();
                    lastValueWrapper.setLastValue(status.getLastValue());
                    lastValueWrapper.setStrLastValue(status.getStrLastValue());
                    lastValueWrapper.setTimeStamp(status.getTime());
                    status.setStrLastValue(SimpleStringUtil.object2json(lastValueWrapper));
                    SQLExecutor.updateWithDBName(statusDBConf.getPoolname(),updateSqlHis,status.getStrLastValue(),status.getId());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }
}
