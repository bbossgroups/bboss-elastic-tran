package org.frameworkset.tran.input.zipfile;

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;

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
    
    /**
     * 解压加密的ZIP文件
     * @param zipFilePath ZIP文件路径
     * @param destDirectory 解压目标目录
     * @param password 密码
     * @throws ZipException ZIP异常
     */
    public static void extractEncryptedZip(File zipFilePath, String destDirectory, String password) throws ZipException {
        ZipFile zipFile = new ZipFile(zipFilePath);
        if (zipFile.isEncrypted()) {
            zipFile.setPassword(password.toCharArray());
        }
        zipFile.extractAll(destDirectory);
    }

    /**
     * 解压加密的ZIP文件
     * @param zipFilePath ZIP文件路径
     * @param destDirectory 解压目标目录
     * @param password 密码
     * @throws ZipException ZIP异常
     */
    public static void extractEncryptedZip(String zipFilePath, String destDirectory, String password) throws ZipException {
        extractEncryptedZip(new File( zipFilePath),  destDirectory,  password);
    }
    
    /**
     * 检查ZIP文件是否加密
     * @param zipFilePath ZIP文件路径
     * @return 是否加密
     * @throws ZipException ZIP异常
     */
    public static boolean isEncrypted(String zipFilePath) throws ZipException {
        ZipFile zipFile = new ZipFile(zipFilePath);
        return zipFile.isEncrypted();
    }
}

