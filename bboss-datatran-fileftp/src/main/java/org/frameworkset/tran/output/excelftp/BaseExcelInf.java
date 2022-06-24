package org.frameworkset.tran.output.excelftp;
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

import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.frameworkset.tran.input.excel.CellMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/5/24 14:26
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseExcelInf {

	private static final Logger log = LoggerFactory.getLogger(BaseExcelInf.class);
	private static final String DEFAULT_URL_ENCODING = "UTF-8";
	/**
	 * 工作薄对象
	 */
	protected SXSSFWorkbook wb;
	/**
	 * 列索引、列名称、列对应的字段field名称映射关系
	 */
	private List<CellMapping> cellMappingList;
	public SXSSFWorkbook getWb(){
		return wb;
	}
	/**
	 * 输出数据流
	 * @param wb
	 * @param filePath 输出数据流
	 */
	public static void write(SXSSFWorkbook wb, String filePath) throws IOException {
		if(null == wb){
			throw new IOException("Null SXSSFWorkbook Object");
		}
		FileOutputStream outputStream = null;
		try {
			outputStream = new FileOutputStream(filePath);
			wb.write(outputStream);
		}
		finally {
			if(outputStream != null ){
				outputStream.close();
			}
		}
	}
//	/**
//	 * 输出到客户端
//	 *
//	 * @param fileName 输出文件名
//	 */
//	public  void write(HttpServletResponse response, String fileName) throws IOException {
//		response.reset();
//		response.setContentType("application/octet-stream; charset=utf-8");
//		response.setHeader("Content-Disposition", "attachment; filename=" + URLEncoder.encode(fileName,
//				DEFAULT_URL_ENCODING));
//		write(response.getOutputStream());
//	}
	/**
	 * 输出数据流
	 *
	 * @param os 输出数据流
	 */
	public void write(OutputStream os) throws IOException {
		wb.write(os);
	}

	/**
	 * 输出数据流
	 *
	 * @param filePath 输出数据流
	 */
	public void write(File filePath) throws IOException {
		FileOutputStream outputStream = null;
		try {
			outputStream = new FileOutputStream(filePath);
			wb.write(outputStream);
		}
		finally {
			if(outputStream != null ){
				outputStream.close();
			}
		}

	}


	public List<CellMapping> getCellMappingList() {
		return cellMappingList;
	}
}
