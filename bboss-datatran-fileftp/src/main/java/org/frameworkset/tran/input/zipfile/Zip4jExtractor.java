package org.frameworkset.tran.input.zipfile;

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
/**
 * Copyright 2025 bboss
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

/**
 * @author biaoping.yin
 * @Date 2025/9/22
 */


public class Zip4jExtractor {
    private static Logger logger = LoggerFactory.getLogger(Zip4jExtractor.class);
    public static void main(String[] args) throws Exception { 
        String zipFilePath = "C:\\data\\zipfile\\behavior_event_02_20251014143006.zip";

        String password = "123456";
        int files = getZipFileCount(zipFilePath);
        
        files = extractEncryptedZip(zipFilePath, "C:\\data\\unzipfile\\", password);
        System.out.println("ZIP文件包含" + files + "个文件");
    }
    
    /**
     * 解压加密的ZIP文件
     * @param zipFilePath ZIP文件路径
     * @param destDirectory 解压目标目录
     * @param password 密码
     * @return 解压文件数量                
     * @throws ZipException ZIP异常
     */
    public static int extractEncryptedZip(File zipFilePath, String destDirectory, String password) throws ZipException {
        ZipFile zipFile = null;
        try {
            zipFile = new ZipFile(zipFilePath);
            if (zipFile.isEncrypted()) {
                zipFile.setPassword(password.toCharArray());
            }
            int files = zipFile.getFileHeaders().size();
//        // 检查ZIP文件是否分卷
//        if(zipFile.isSplitArchive()){
//            // 合并分卷文件
//        }
            zipFile.extractAll(destDirectory);
            return files;
        }
        finally {
            if(zipFile != null){
                try{
                    zipFile.close();
                }catch (Exception e){
                     
                }
            }
        }
    }

    /**
     * 获取zip包中的文件数量
     * @param zipFilePath ZIP文件路径
     * @return 解压文件数量
     */
    public static int getZipFileCount(String zipFilePath) throws ZipException {
        ZipFile zipFile = null;
        try {
            zipFile = new ZipFile(zipFilePath);
            int files = zipFile.getFileHeaders().size();
            return files;
        }
        finally {
            if(zipFile != null){
                try{
                    zipFile.close();
                }catch (Exception e){
                     
                }
            }
        }
    }

    /**
     * 解压加密的ZIP文件
     * @param zipFilePath ZIP文件路径
     * @param destDirectory 解压目标目录
     * @param password 密码
     * @return 解压文件数量
     * @throws ZipException ZIP异常
     */
    public static int extractEncryptedZip(String zipFilePath, String destDirectory, String password) throws ZipException {
        return extractEncryptedZip(new File( zipFilePath),  destDirectory,  password);
    }
    
    /**
     * 检查ZIP文件是否加密
     * @param zipFilePath ZIP文件路径
     * @return 是否加密
     * @throws ZipException ZIP异常
     */
    public static boolean isEncrypted(String zipFilePath) throws ZipException {

        ZipFile zipFile = null;
        try {
            zipFile = new ZipFile(zipFilePath);
            return zipFile.isEncrypted();
        }
        finally {
            if(zipFile != null){
                try{
                    zipFile.close();
                }catch (Exception e){

                }
            }
        }
    }
}

