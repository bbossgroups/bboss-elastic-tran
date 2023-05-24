package org.frameworkset.tran.plugin.db.output;
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

import org.frameworkset.tran.plugin.db.TranSQLInfo;

/**
 * <p>Description: 用于配置多表sql配置信息</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/5/24
 * @author biaoping.yin
 * @version 1.0
 */
public class SQLConf {
    private String insertSql;
    private String insertSqlName;

    private String updateSql;
    private String updateSqlName;

    private String deleteSql;
    private String deleteSqlName;
    private TranSQLInfo targetSqlInfo;
    private TranSQLInfo targetUpdateSqlInfo;
    private TranSQLInfo targetDeleteSqlInfo;

    public TranSQLInfo getTargetSqlInfo() {
        return targetSqlInfo;
    }

    public void setTargetSqlInfo(TranSQLInfo targetSqlInfo) {
        this.targetSqlInfo = targetSqlInfo;
    }

    public TranSQLInfo getTargetUpdateSqlInfo() {
        return targetUpdateSqlInfo;
    }

    public void setTargetUpdateSqlInfo(TranSQLInfo targetUpdateSqlInfo) {
        this.targetUpdateSqlInfo = targetUpdateSqlInfo;
    }

    public TranSQLInfo getTargetDeleteSqlInfo() {
        return targetDeleteSqlInfo;
    }

    public void setTargetDeleteSqlInfo(TranSQLInfo targetDeleteSqlInfo) {
        this.targetDeleteSqlInfo = targetDeleteSqlInfo;
    }

    public String getUpdateSql() {
        return updateSql;
    }

    public void setUpdateSql(String updateSql) {
        this.updateSql = updateSql;
    }

    public String getUpdateSqlName() {
        return updateSqlName;
    }

    public void setUpdateSqlName(String updateSqlName) {
        this.updateSqlName = updateSqlName;
    }

    public String getDeleteSql() {
        return deleteSql;
    }

    public void setDeleteSql(String deleteSql) {
        this.deleteSql = deleteSql;
    }

    public String getDeleteSqlName() {
        return deleteSqlName;
    }

    public void setDeleteSqlName(String deleteSqlName) {
        this.deleteSqlName = deleteSqlName;
    }

    public String getInsertSql() {
        return insertSql;
    }

    public void setInsertSql(String insertSql) {
        this.insertSql = insertSql;
    }

    public String getInsertSqlName() {
        return insertSqlName;
    }

    public void setInsertSqlName(String insertSqlName) {
        this.insertSqlName = insertSqlName;
    }
}
