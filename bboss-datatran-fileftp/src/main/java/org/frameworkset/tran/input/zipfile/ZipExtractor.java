package org.frameworkset.tran.input.zipfile;
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
 import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipExtractor {
    
    /**
     * 解压ZIP文件到指定目录
     * @param zipFilePath ZIP文件路径
     * @param destDirectory 解压目标目录
     * @throws IOException IO异常
     */
    public static void unzip(String zipFilePath, String destDirectory) throws IOException {
        File destDir = new File(destDirectory);
        if (!destDir.exists()) {
            destDir.mkdir();
        }
        
        try (FileInputStream fis = new FileInputStream(zipFilePath);
             ZipInputStream zis = new ZipInputStream(fis)) {
            
            ZipEntry entry = zis.getNextEntry();
            while (entry != null) {
                String fileName = entry.getName();
                File newFile = new File(destDirectory + File.separator + fileName);
                
                // 防止ZIP条目路径遍历漏洞
                if (!newFile.getCanonicalPath().startsWith(destDirectory)) {
                    throw new IOException("Entry is outside of the target dir: " + fileName);
                }
                
                if (entry.isDirectory()) {
                    newFile.mkdirs();
                } else {
                    // 创建父目录
                    new File(newFile.getParent()).mkdirs();
                    
                    // 写入文件
                    try (FileOutputStream fos = new FileOutputStream(newFile)) {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                }
                zis.closeEntry();
                entry = zis.getNextEntry();
            }
        }
    }
}

