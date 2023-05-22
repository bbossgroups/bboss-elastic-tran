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

import org.frameworkset.util.annotations.DateFormateMeta;

public class FieldMeta {
	private String targetFieldName;
	private String sourceFieldName;
	private DateFormateMeta dateFormateMeta;
	private Boolean ignore ;
	private Object value;
	public FieldMeta(){

	}
	public FieldMeta(String targetFieldName, Object value){
		this.targetFieldName = targetFieldName;
		this.value = value;
	}
	public FieldMeta(String targetFieldName, DateFormateMeta dateFormat , Object value){
		this.targetFieldName = targetFieldName;
		this.value = value;
		this.dateFormateMeta = dateFormat;

	}
	public String getTargetFieldName() {
		return targetFieldName;
	}

	public void setTargetFieldName(String targetFieldName) {
		this.targetFieldName = targetFieldName;
	}

	public String getSourceFieldName() {
		return sourceFieldName;
	}

	public void setSourceFieldName(String sourceFieldName) {
		this.sourceFieldName = sourceFieldName;
	}

	public DateFormateMeta getDateFormateMeta() {
		return dateFormateMeta;
	}

	public void setDateFormateMeta(DateFormateMeta dateFormateMeta) {
		this.dateFormateMeta = dateFormateMeta;
	}


	public Boolean getIgnore() {
		return ignore;
	}

	public void setIgnore(Boolean ignore) {
		this.ignore = ignore;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
}
