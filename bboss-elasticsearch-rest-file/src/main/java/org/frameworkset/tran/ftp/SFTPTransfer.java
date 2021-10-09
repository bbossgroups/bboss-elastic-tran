package org.frameworkset.tran.ftp;
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

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteResourceFilter;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.UserAuthException;
import net.schmizz.sshj.xfer.FileSystemFile;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FtpFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/1 20:27
 * @author biaoping.yin
 * @version 1.0
 */
public class SFTPTransfer {
	private static Logger logger = LoggerFactory.getLogger(SFTPTransfer.class);

	public static List<RemoteResourceInfo> ls(final FtpContext fileFtpOupputContext){
		final List<RemoteResourceInfo> files = new ArrayList<RemoteResourceInfo>();
		final FileFilter fileFilter = fileFtpOupputContext.getFileFilter();
		final FtpFileFilter ftpFileFilter = fileFtpOupputContext.getFtpFileFilter();
		final FileConfig fileConfig = fileFtpOupputContext.getFtpConfig();
		handlerFile(  fileFtpOupputContext, new SFTPAction (){
			@Override
			public void execute(SFTPClient sftp) throws IOException {
//				fileFtpOupputContext.getf
				if(ftpFileFilter != null){
					List<RemoteResourceInfo> _files = sftp.ls(fileFtpOupputContext.getRemoteFileDir(), new RemoteResourceFilter() {
						@Override
						public boolean accept(RemoteResourceInfo resource) {

							return ftpFileFilter.accept(resource, resource.getName(), fileConfig);
						}
					});
					if (_files != null && _files.size() > 0) {
						files.addAll(_files);
					}
				}
				else if(fileFilter != null) {
					List<RemoteResourceInfo> _files = sftp.ls(fileFtpOupputContext.getRemoteFileDir(), new RemoteResourceFilter() {
						@Override
						public boolean accept(RemoteResourceInfo resource) {

							return fileFilter.accept(resource.getParent(), resource.getName(), fileConfig);
						}
					});
					if (_files != null && _files.size() > 0) {
						files.addAll(_files);
					}
				}
				else{
					List<RemoteResourceInfo> _files = sftp.ls(fileFtpOupputContext.getRemoteFileDir());
					if (_files != null && _files.size() > 0) {
						files.addAll(_files);
					}
				}
				if(logger.isDebugEnabled())
					logger.debug("List files in remote dir["+fileFtpOupputContext.getRemoteFileDir()+" from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() );
			}
		},"List files in remote dir["+fileFtpOupputContext.getRemoteFileDir()+" from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + "failed." );
		return files;
	}
	/**
	 * 文件下载
	 * @param fileFtpOupputContext
	 * @param sourceFile
	 * @param destDir
	 */
	public static void downloadFile(final FtpContext fileFtpOupputContext,final String sourceFile,final String destDir){
		handlerFile(  fileFtpOupputContext, new SFTPAction (){

			@Override
			public void execute(SFTPClient sftp) throws IOException {
				sftp.get(sourceFile, new FileSystemFile(destDir));
				if(logger.isInfoEnabled())
					logger.info("Download file "+sourceFile+" from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " success to ["+destDir+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"]");
			}
		}, "Download file "+sourceFile+" from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " failed to ["+destDir+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"]");

	}


	/**
	 * 删除文件
	 * @param fileFtpOupputContext
	 * @param remoteFile
	 */
	public static void deleteFile(final FtpContext fileFtpOupputContext,final String remoteFile){
		handlerFile(  fileFtpOupputContext, new SFTPAction (){

			@Override
			public void execute(SFTPClient sftp) throws IOException {
				sftp.rm(remoteFile);
				if(logger.isInfoEnabled())
					logger.info("Delete file "+remoteFile+" from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " success.");
			}
		}, "Delete file "+remoteFile+" from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " failed.");

	}

	private static void handlerFile(FtpContext fileFtpOupputContext, SFTPAction sftpAction,String errorMessage) {
		final SSHClient ssh = new SSHClient();

		try {

			if(fileFtpOupputContext.getHostKeyVerifiers() != null) {
				ssh.loadKnownHosts();
				for(String hostKey:fileFtpOupputContext.getHostKeyVerifiers())
					ssh.addHostKeyVerifier(hostKey);
			}
			else{
				ssh.addHostKeyVerifier(new PromiscuousVerifier());
			}
			ssh.connect(fileFtpOupputContext.getFtpIP(),fileFtpOupputContext.getFtpPort());
			if(fileFtpOupputContext.getKeepAliveTimeout() > 0 )
				ssh.getConnection().getKeepAlive().setKeepAliveInterval((int)(fileFtpOupputContext.getKeepAliveTimeout() / 1000)); //every 60sec
//            ssh.addHostKeyVerifier(new PromiscuousVerifier());
			ssh.authPassword(fileFtpOupputContext.getFtpUser(),fileFtpOupputContext.getFtpPassword());
			final SFTPClient sftp = ssh.newSFTPClient();
			try {
				sftpAction.execute(sftp);
			} finally {
				sftp.close();
			}
		} catch (UserAuthException e) {
//			logger.error(message,e);
			throw new DataImportException(errorMessage,e);
		} catch (TransportException e) {
//			logger.error(message,e);
			throw new DataImportException(errorMessage,e);
		} catch (IOException e) {
//			logger.error(message,e);
			throw new DataImportException(errorMessage,e);
		} finally {
			try {
				ssh.disconnect();
			} catch (IOException e) {
				logger.warn("ssh " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " disconnect failed:",e);
			}
		}
	}
	/**
	 * 文件上传
	 * @param fileFtpOupputContext
	 * @param filePath
	 */
	public static void sendFile(final FtpContext fileFtpOupputContext, final String filePath){
		handlerFile(  fileFtpOupputContext, new SFTPAction (){

			@Override
			public void execute(SFTPClient sftp) throws IOException {
				sftp.put(new FileSystemFile(filePath), fileFtpOupputContext.getRemoteFileDir());
				if(logger.isInfoEnabled())
					logger.info("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " success:filePath["+filePath+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"]");
			}
		}, "Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " failed:filePath["+filePath+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"]");

	}

}
