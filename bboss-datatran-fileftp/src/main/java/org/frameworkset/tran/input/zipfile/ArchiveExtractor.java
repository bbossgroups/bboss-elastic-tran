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
 * @Date 2025/11/13
 */

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class ArchiveExtractor {
    
    /**
     * 解压文件主方法，根据文件扩展名自动选择解压方式
     * 
     * @param tarFilePath 源压缩文件路径
     * @param destinationDir 目标解压目录
     * @throws IOException IO异常
     */
    public static int extract(File tarFilePath, String destinationDir) throws IOException {
        String lowerCaseFileName = tarFilePath.getName().toLowerCase();
        
        if (lowerCaseFileName.endsWith(".tar.gz") || lowerCaseFileName.endsWith(".tgz")) {
            return extractTarGz(tarFilePath, destinationDir);
        } else if (lowerCaseFileName.endsWith(".tar")) {
            return extractTar(tarFilePath, destinationDir);
        } else if (lowerCaseFileName.endsWith(".gz")) {
            return extractGz(tarFilePath, destinationDir);
        } else {
            throw new IllegalArgumentException("不支持的文件格式: " + lowerCaseFileName);
        }
    }

    /**
     * 解压文件主方法，根据文件扩展名自动选择解压方式
     *
     * @param tarFilePath 源压缩文件路径
     * @param destinationDir 目标解压目录
     * @throws IOException IO异常
     */
    public static int extract(String tarFilePath, String destinationDir) throws IOException {
        return extract(new File(tarFilePath),  destinationDir);
    }
    
    /**
     * 解压tar.gz文件
     * 
     * @param sourceFile 源tar.gz文件路径
     * @param destinationDir 目标解压目录
     * @throws IOException IO异常
     */
    public static int extractTarGz(File  sourceFile, String destinationDir) throws IOException {
        int files = 0;
        try (FileInputStream fis = new FileInputStream(sourceFile);
             GzipCompressorInputStream gzis = new GzipCompressorInputStream(fis);
             TarArchiveInputStream tais = new TarArchiveInputStream(gzis)) {
            
            TarArchiveEntry entry;
            while ((entry = tais.getNextTarEntry()) != null) {
                File outputFile = new File(destinationDir, entry.getName());
                
                // 防止路径遍历漏洞
                if (!outputFile.getCanonicalPath().startsWith(new File(destinationDir).getCanonicalPath())) {
                    throw new IOException("Entry is outside of the target directory: " + entry.getName());
                }
                files ++;
                if (entry.isDirectory()) {
                    if (!outputFile.exists()) {
                        outputFile.mkdirs();
                    }
                } else {
                    // 创建父目录
                    File parent = outputFile.getParentFile();
                    if (parent != null && !parent.exists()) {
                        parent.mkdirs();
                    }
                    
                    try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                        IOUtils.copy(tais, fos);
                    }
                    
                
                }
            }
        }
        return files;
    }
    
    /**
     * 解压tar文件
     * 
     * @param sourceFile 源tar文件路径
     * @param destinationDir 目标解压目录
     * @throws IOException IO异常
     */
    public static int extractTar(File sourceFile, String destinationDir) throws IOException {
        int files = 0;
        try (FileInputStream fis = new FileInputStream(sourceFile);
             TarArchiveInputStream tais = new TarArchiveInputStream(fis)) {
            
            TarArchiveEntry entry;
            while ((entry = tais.getNextTarEntry()) != null) {
               
                File outputFile = new File(destinationDir, entry.getName());
                
                // 防止路径遍历漏洞
                if (!outputFile.getCanonicalPath().startsWith(new File(destinationDir).getCanonicalPath())) {
                    throw new IOException("Entry is outside of the target directory: " + entry.getName());
                }
                files ++;
                if (entry.isDirectory()) {
                    if (!outputFile.exists()) {
                        outputFile.mkdirs();
                    }
                } else {
                    // 创建父目录
                    File parent = outputFile.getParentFile();
                    if (parent != null && !parent.exists()) {
                        parent.mkdirs();
                    }
                    
                    try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                        IOUtils.copy(tais, fos);
                    }
                    
                 
                }
            }
        }
        return files;
    }
    
    /**
     * 解压gz文件（单个文件）
     * 
     * @param sourceFile 源gz文件路径
     * @param destinationDir 目标解压目录
     * @throws IOException IO异常
     */
    public static int  extractGz(File sourceFile, String destinationDir) throws IOException {
        // 对于.gz文件，通常是对单个文件的压缩，去掉.gz后缀作为输出文件名
        String fileName = sourceFile.getName();
        int files = 0;
        String outputFileName = fileName.substring(0, fileName.length() - 3);
        File outputFile = new File(destinationDir, new File(outputFileName).getName());
        
        try (FileInputStream fis = new FileInputStream(sourceFile);
             GzipCompressorInputStream gzis = new GzipCompressorInputStream(fis);
             FileOutputStream fos = new FileOutputStream(outputFile)) {
            
            IOUtils.copy(gzis, fos);
            files ++;
        }
        return files;
    }
}

