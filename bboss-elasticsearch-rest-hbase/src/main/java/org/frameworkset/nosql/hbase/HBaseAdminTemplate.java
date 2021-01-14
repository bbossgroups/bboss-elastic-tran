/*
 * Copyright 2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.frameworkset.nosql.hbase;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;


/**
 * @author emeroad
 * @author HyunGil Jeong
 */
public class HBaseAdminTemplate {

    private final AdminFactory adminFactory;

    public HBaseAdminTemplate(AdminFactory adminFactory) {
        this.adminFactory = Objects.requireNonNull(adminFactory, "adminFactory must not be null");
    }

    public boolean createTableIfNotExists(final HTableDescriptor htd) {
        return execute(new AdminCallback<Boolean>() {
            @Override
            public Boolean doInAdmin(Admin admin) throws Throwable {
                TableName tableName = htd.getTableName();
                if (!admin.tableExists(tableName)) {
                    admin.createTable(htd);
                    return true;
                }
                return false;
            }
        });
    }

    public boolean tableExists(final TableName tableName) {
        return execute(new AdminCallback<Boolean>() {
            @Override
            public Boolean doInAdmin(Admin admin) throws Throwable {
                return admin.tableExists(tableName);
            }
        });
    }

    public boolean dropTableIfExists(final TableName tableName) {
        return execute(new AdminCallback<Boolean>() {
            @Override
            public Boolean doInAdmin(Admin admin) throws Throwable {
                if (admin.tableExists(tableName)) {
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                    return true;
                }
                return false;
            }
        });
    }

    public void dropTable(final TableName tableName) {
        execute(new AdminCallback<Void>() {
            @Override
            public Void doInAdmin(Admin admin) throws Throwable {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                return null;
            }
        });
    }

    public final <T> T execute(AdminCallback<T> action) {
        Objects.requireNonNull(action, "action must not be null");
        Admin admin = adminFactory.getAdmin();
        try {
            return action.doInAdmin(admin);
        } catch (Throwable e) {
            if (e instanceof Error) {
                throw (Error) e;
            }
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new HbaseSystemException((Exception) e);
        } finally {
            adminFactory.releaseAdmin(admin);
        }
    }
}
