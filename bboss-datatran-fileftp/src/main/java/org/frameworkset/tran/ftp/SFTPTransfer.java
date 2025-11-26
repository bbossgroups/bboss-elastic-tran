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

import com.frameworkset.util.SimpleStringUtil;
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
import org.frameworkset.tran.input.file.SFTPFilterFileInfo;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.scan.JobFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
        final JobFileFilter jobFileFilter = fileFtpOupputContext.getJobFileFilter();
		final FileConfig fileConfig = fileFtpOupputContext.getFileConfig();
		handlerFile(  fileFtpOupputContext, new SFTPAction (){
			@Override
			public void execute(SFTPClient sftp) throws IOException {
//				fileFtpOupputContext.getf
				if(logger.isDebugEnabled())
					logger.debug("List files in remote dir["+fileFtpOupputContext.getRemoteFileDir()+"} from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() +" start......");
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

							return fileFilter.accept(new SFTPFilterFileInfo(resource), fileConfig);
						}
					});
					if (_files != null && _files.size() > 0) {
						files.addAll(_files);
					}
				}

                else if(jobFileFilter != null) {
                    List<RemoteResourceInfo> _files = sftp.ls(fileFtpOupputContext.getRemoteFileDir(), new RemoteResourceFilter() {
                        @Override
                        public boolean accept(RemoteResourceInfo resource) {

                            return jobFileFilter.accept(new SFTPFilterFileInfo(resource), fileFtpOupputContext.getJobFlowNodeExecuteContext());
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
					logger.debug("List files in remote dir["+fileFtpOupputContext.getRemoteFileDir()+"} from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() +" complete.");
			}
		},"List files in remote dir["+fileFtpOupputContext.getRemoteFileDir()+"] from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " failed:" );
		return files;
	}

	public static List<RemoteResourceInfo> ls(String remotePath ,final FtpContext fileFtpOupputContext){
		final List<RemoteResourceInfo> files = new ArrayList<RemoteResourceInfo>();
		final FileFilter fileFilter = fileFtpOupputContext.getFileFilter();
		final FtpFileFilter ftpFileFilter = fileFtpOupputContext.getFtpFileFilter();
        final JobFileFilter jobFileFilter = fileFtpOupputContext.getJobFileFilter();

        final JobFlowNodeExecuteContext jobFlowNodeExecuteContext = fileFtpOupputContext.getJobFlowNodeExecuteContext();
		final FileConfig fileConfig = fileFtpOupputContext.getFileConfig();
		handlerFile(  fileFtpOupputContext, new SFTPAction (){
			@Override
			public void execute(SFTPClient sftp) throws IOException {
//				fileFtpOupputContext.getf
				if(logger.isDebugEnabled())
					logger.debug("List files in remote dir["+remotePath+"} from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() +" start......");
				if(ftpFileFilter != null){
					List<RemoteResourceInfo> _files = sftp.ls(remotePath, new RemoteResourceFilter() {
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
					List<RemoteResourceInfo> _files = sftp.ls(remotePath, new RemoteResourceFilter() {
						@Override
						public boolean accept(RemoteResourceInfo resource) {
                            
							return fileFilter.accept(new SFTPFilterFileInfo(resource), fileConfig);
						}
					});
					if (_files != null && _files.size() > 0) {
						files.addAll(_files);
					}
				}
                else if(jobFileFilter != null){
                    List<RemoteResourceInfo> _files = sftp.ls(remotePath, new RemoteResourceFilter() {
                        @Override
                        public boolean accept(RemoteResourceInfo resource) {

                            return jobFileFilter.accept(new SFTPFilterFileInfo(resource), jobFlowNodeExecuteContext);
                        }
                    });
                    if (_files != null && _files.size() > 0) {
                        files.addAll(_files);
                    }
                }
				else{
					List<RemoteResourceInfo> _files = sftp.ls(remotePath);
					if (_files != null && _files.size() > 0) {
						files.addAll(_files);
					}
				}
				if(logger.isDebugEnabled())
					logger.debug("List files in remote dir["+remotePath+"} from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() +" complete.");
			}
		},"List files in remote dir["+remotePath+"] from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " failed:" );
		return files;
	}

	public static List<RemoteResourceInfo> ls(final RemoteResourceInfo parent,final FtpContext fileFtpOupputContext){
		final List<RemoteResourceInfo> files = new ArrayList<RemoteResourceInfo>();
		final FileFilter fileFilter = fileFtpOupputContext.getFileFilter();
        
        final FtpFileFilter ftpFileFilter = fileFtpOupputContext.getFtpFileFilter();
		final FileConfig fileConfig = fileFtpOupputContext.getFileConfig();
        final JobFileFilter jobFileFilter = fileFtpOupputContext.getJobFileFilter();

        final JobFlowNodeExecuteContext jobFlowNodeExecuteContext = fileFtpOupputContext.getJobFlowNodeExecuteContext();
		handlerFile(  fileFtpOupputContext, new SFTPAction (){
			@Override
			public void execute(SFTPClient sftp) throws IOException {
				if(logger.isDebugEnabled())
					logger.debug("List files in remote dir["+parent.getPath()+"} from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() +" start......" );
//				fileFtpOupputContext.getf
				if(ftpFileFilter != null){
					List<RemoteResourceInfo> _files = sftp.ls(parent.getPath(), new RemoteResourceFilter() {
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
					List<RemoteResourceInfo> _files = sftp.ls(parent.getPath(), new RemoteResourceFilter() {
						@Override
						public boolean accept(RemoteResourceInfo resource) {

                            return fileFilter.accept(new SFTPFilterFileInfo(resource), fileConfig);
						}
					});
					if (_files != null && _files.size() > 0) {
						files.addAll(_files);
					}
				}
                else if(jobFileFilter != null){
                    List<RemoteResourceInfo> _files = sftp.ls(parent.getPath(), new RemoteResourceFilter() {
                        @Override
                        public boolean accept(RemoteResourceInfo resource) {

                             return jobFileFilter.accept(new SFTPFilterFileInfo(resource), jobFlowNodeExecuteContext);
                        }
                    });
                    if (_files != null && _files.size() > 0) {
                        files.addAll(_files);
                    }
                }
				else{
					List<RemoteResourceInfo> _files = sftp.ls(parent.getPath());
					if (_files != null && _files.size() > 0) {
						files.addAll(_files);
					}
				}
				if(logger.isDebugEnabled())
					logger.debug("List files in remote dir["+parent.getPath()+"} from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() +" complete." );
			}
		},"List files in remote dir["+parent.getPath()+"] from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " failed." );
		return files;
	}
	/**
	 * 文件下载
	 * @param fileFtpOupputContext
	 * @param sourceFile
	 * @param destDir
	 */
	public static void downloadFile(final FtpContext fileFtpOupputContext,final String sourceFile,final String destDir){
		String failedMess = null;
		boolean hasRemoteFileDir = fileFtpOupputContext != null && SimpleStringUtil.isNotEmpty(fileFtpOupputContext.getRemoteFileDir());
		if(hasRemoteFileDir) {
			failedMess = "Download file "+sourceFile+" from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " to ["+destDir+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"] failed:";
		}
		else{
			failedMess = "Download file "+sourceFile+" from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " to ["+destDir+"] failed:";
		}
		handlerFile(  fileFtpOupputContext, new SFTPAction (){

			@Override
			public void execute(SFTPClient sftp) throws IOException {
				if(logger.isInfoEnabled()) {
					if(hasRemoteFileDir) {
						logger.info("Download file " + sourceFile + " from sftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " to [" + destDir + "],remote dir[" + fileFtpOupputContext.getRemoteFileDir() + "] start......");
					}
					else{
						logger.info("Download file " + sourceFile + " from sftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " to [" + destDir + "] start......");
					}
				}
				long startTime = System.currentTimeMillis();
				sftp.get(sourceFile, new FileSystemFile(destDir));
				long endTime = System.currentTimeMillis();
				if(logger.isInfoEnabled()) {
					if(hasRemoteFileDir) {
						logger.info("Download file " + sourceFile + " from sftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " to [" + destDir + "],remote dir[" + fileFtpOupputContext.getRemoteFileDir() + "] complete, elapsed time:" + (endTime - startTime) + " 毫秒.");
					}
					else{
						logger.info("Download file " + sourceFile + " from sftp " + fileFtpOupputContext.getFtpIP() + ":" + fileFtpOupputContext.getFtpPort() + " to [" + destDir + "] complete, elapsed time:" + (endTime - startTime) + " 毫秒.");
					}
				}
			}
		}, failedMess);

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
				if(logger.isInfoEnabled())
					logger.info("Delete file "+remoteFile+" from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " start......");
				sftp.rm(remoteFile);
				if(logger.isInfoEnabled())
					logger.info("Delete file "+remoteFile+" from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " complete.");
			}
		}, "Delete file "+remoteFile+" from sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " failed:");

	}

	private static void handlerFile(FtpContext fileFtpOupputContext, SFTPAction sftpAction,String errorMessage) {
		final SSHClient ssh = new SSHClient();

		try {

			if(fileFtpOupputContext.getHostKeyVerifiers() != null) {
				ssh.loadKnownHosts();
				for(Object hostKey:fileFtpOupputContext.getHostKeyVerifiers())
					ssh.addHostKeyVerifier((String)hostKey);
			}
			else{
				ssh.addHostKeyVerifier(new PromiscuousVerifier());
			}
			ssh.connect(fileFtpOupputContext.getFtpIP(),fileFtpOupputContext.getFtpPort());
			if(fileFtpOupputContext.getKeepAliveTimeout() > 0L )
				ssh.getConnection().getKeepAlive().setKeepAliveInterval((int)(fileFtpOupputContext.getKeepAliveTimeout() / 1000)); //every 60sec
			if(fileFtpOupputContext.getSocketTimeout() > 0L )
				ssh.setTimeout((int)fileFtpOupputContext.getSocketTimeout()); //every 60sec
			if(fileFtpOupputContext.getConnectTimeout() > 0L )
				ssh.setConnectTimeout((int)fileFtpOupputContext.getConnectTimeout()); //every 60sec



//            ssh.addHostKeyVerifier(new PromiscuousVerifier());
			ssh.authPassword(fileFtpOupputContext.getFtpUser(),fileFtpOupputContext.getFtpPassword());
			final SFTPClient sftp = ssh.newSFTPClient();
			try {
				sftpAction.execute(sftp);
			} finally {
				sftp.close();
			}
		} catch (UserAuthException e) {
			throw new DataImportException(errorMessage,e);
		} catch (TransportException e) {
			throw new DataImportException(errorMessage,e);
		} catch (IOException e) {
			throw new DataImportException(errorMessage,e);
		}  catch (DataImportException e) {
            throw e;
        } catch (Exception e) {
            throw new DataImportException(errorMessage,e);
        } finally {
			try {
				ssh.disconnect();
			} catch (Exception e) {
				logger.warn("ssh " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " disconnect failed:",e);
			}
		}
	}
	public static long getLastTime(RemoteResourceInfo remoteResourceInfo){
		return remoteResourceInfo.getAttributes().getAtime() > remoteResourceInfo.getAttributes().getMtime()?
				remoteResourceInfo.getAttributes().getAtime():remoteResourceInfo.getAttributes().getMtime();
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

				if(logger.isInfoEnabled())
					logger.info("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " ,filePath["+filePath+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"] start ......");
				long startTime = System.currentTimeMillis();
				sftp.put(new FileSystemFile(filePath), fileFtpOupputContext.getRemoteFileDir());
				long endTime = System.currentTimeMillis();
				if(logger.isInfoEnabled())
					logger.info("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " ,filePath["+filePath+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"] complete, elapsed time:"+(endTime - startTime) + " 毫秒.");
			}
		}, "Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " ,filePath["+filePath+"],remote dir["+fileFtpOupputContext.getRemoteFileDir()+"] failed:");

	}

	/**
	 * 文件上传
	 * @param fileFtpOupputContext
	 * @param filePath
	 */
	public static void sendFile(final FtpContext fileFtpOupputContext, final String filePath,final String remoteDir){
		handlerFile(  fileFtpOupputContext, new SFTPAction (){

			@Override
			public void execute(SFTPClient sftp) throws IOException {

				if(logger.isInfoEnabled())
					logger.info("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " ,filePath["+filePath+"],remote dir["+remoteDir+"] start ......");
				long startTime = System.currentTimeMillis();
				sftp.put(new FileSystemFile(filePath), remoteDir);
				long endTime = System.currentTimeMillis();
				if(logger.isInfoEnabled())
					logger.info("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " ,filePath["+filePath+"],remote dir["+remoteDir+"] complete, elapsed time:"+(endTime - startTime) + " 毫秒.");
			}
		}, "Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " ,filePath["+filePath+"],remote dir["+remoteDir+"] failed:");

	}

	/**
	 * 文件上传
	 * @param fileFtpOupputContext
	 * @param file
	 */
	public static void sendFile(final FtpContext fileFtpOupputContext, final File file, final String remoteDir){
		handlerFile(  fileFtpOupputContext, new SFTPAction (){

			@Override
			public void execute(SFTPClient sftp) throws IOException {

				if(logger.isInfoEnabled())
					logger.info("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " ,filePath["+file.getAbsolutePath()+"],remote dir["+remoteDir+"] start ......");
				long startTime = System.currentTimeMillis();
				sftp.put(new FileSystemFile(file), remoteDir);
				long endTime = System.currentTimeMillis();
				if(logger.isInfoEnabled())
					logger.info("Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " ,filePath["+file.getAbsolutePath()+"],remote dir["+remoteDir+"] complete, elapsed time:"+(endTime - startTime) + " 毫秒.");
			}
		}, "Send file to sftp " + fileFtpOupputContext.getFtpIP()+":"+fileFtpOupputContext.getFtpPort() + " ,filePath["+file.getAbsolutePath()+"],remote dir["+remoteDir+"] failed:");

	}

}
