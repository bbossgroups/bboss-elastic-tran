package org.frameworkset.tran.plugin.db.input;
/**
 * Copyright 2024 bboss
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

import com.frameworkset.common.poolman.ResultMap;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.record.RecordBuidler;
import org.frameworkset.tran.record.RecordBuidlerContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: 默认记录构建器</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/5/16
 */
public class DBRecordBuilder implements RecordBuidler<ResultSet> {

    @Override
    public Map<String, Object> build(RecordBuidlerContext<ResultSet> recordBuidlerContext) throws DataImportException {
        DBRecordBuilderContext dbRecordBuilderContext = (DBRecordBuilderContext)recordBuidlerContext;
        try {
            return   ResultMap.buildValueObject(  dbRecordBuilderContext.getResultSet(),
                    LinkedHashMap.class,
                    dbRecordBuilderContext.getStatementInfo()) ;
        } catch (SQLException e) {
            throw ImportExceptionUtil.buildDataImportException(dbRecordBuilderContext.getImportContext(),e);
        }
    }
}
