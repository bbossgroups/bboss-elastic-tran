package org.frameworkset.tran;/*
 *  Copyright 2008 biaoping.yin
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


import java.util.Date;

public interface TranResultSet {

	public Object getValue(  int i, String colName,int sqlType) throws ESDataImportException;

	public Object getValue( String colName) throws ESDataImportException;
	public Object getLastValue(String colName) throws ESDataImportException;

	public Object getValue( String colName,int sqlType) throws ESDataImportException;

	public Date getDateTimeValue(String colName) throws ESDataImportException;

	/**
	 * 如果返回null，说明是强制fush操作，true表示有数据，false表示没有数据
	 * @return
	 * @throws ESDataImportException
	 */
	public Boolean next() throws ESDataImportException ;
	public TranMeta getMetaData();

	Object getRecord();

	void stop();

	Object getMetaValue(String fieldName);
}
