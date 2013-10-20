package org.maggiefs.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Wrapper for maggiefs for use by Hadoop.
 * 
 * fs.default.name should be of the form mfs://nnhost:nnport/localMountPoint.
 * This allows us to avoid any further configuration, by either delegating to
 * our namenode or our local mount for all FS operations.
 * 
 */
public class MaggieFileSystem extends FileSystem {
	// used by hadoop
	private Path workingDir;
	private URI name;
	private FileSystem raw;
	// used by us
	private String mountPath;
	private int peerWebPort;
  	private final HttpClient httpClient;
	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("mfs-default.xml");
	}

	public MaggieFileSystem() {
		MultiThreadedHttpConnectionManager connectionManager = 
	      		new MultiThreadedHttpConnectionManager();
		this.httpClient = new HttpClient(connectionManager);
	}

	/** Adds the mountpoint in front of the path, changes scheme to file */
	private Path lookup(Path path) {

		System.out.println("Looking up path " + path);
		String p = path.toUri().getPath();
		p = this.mountPath + p;
		Path ret = new Path("file", null, p);
		System.out.println("Ret: " + ret);
		return ret;
	}

	/* Strips off our mountPoint prefix, ensures scheme is mfs */
	private FileStatus dereference(FileStatus s) {
		return new FileStatus(s.getLen(), s.isDir(), s.getReplication(),
				s.getBlockSize(), s.getModificationTime(), s.getAccessTime(),
				s.getPermission(), s.getOwner(), s.getGroup(),
				dereference(s.getPath()));
	}

	private Path dereference(Path p) {
		String path = p.toUri().getPath();

		if (path.startsWith(mountPath)) {
			path = path.replace(mountPath, "/");
		}
		return new Path("mfs", "localhost:" + peerWebPort, path);
	}

	public URI getUri() {
		return name;
	}

	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		setConf(conf);
		this.raw = FileSystem.getLocal(conf).getRaw();
		this.name = uri;
		if (!uri.getPath().startsWith("/")) {
			throw new RuntimeException("Mountpoint must be absolute!");
		}
//		this.mountPoint = new Path("file", null, uri.getPath());
		this.mountPath = uri.getPath() + "/";
		this.peerWebPort = uri.getPort();
		this.workingDir = new Path(System.getProperty("user.dir")).makeQualified(this);
	}
	
	
	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
			long len) throws IOException {
		// resolve symlinks
		Path fullSysPath = lookup(file.getPath());
		File absFile = new File(fullSysPath.toUri().getPath());
		File resolvedFile = absFile.getCanonicalFile();
		
		// throw error if not still in filesystem
		if (! resolvedFile.toString().startsWith(this.mountPath)) {
			throw new IOException("Symlink resolved to non mountpoint path " + resolvedFile.toString());
		}
		// pop off mountpoint
		// json endpoint expects relative path from mountpoint
		String mountRelFile = resolvedFile.toString().substring(this.mountPath.length());
		
		// get block locations from json endpoint
		GetMethod get = new GetMethod("http://localhost:" + peerWebPort + "/blockLocations?" + 
				"file=" + mountRelFile + 
				"&offset=" + start);
		byte[] respBody = "[]".getBytes();
		try {
			httpClient.executeMethod(get);
			respBody = get.getResponseBody();
		} finally {
			get.releaseConnection();
		}
		// parse from json
		ObjectMapper m = new ObjectMapper();
		String[] hostnames = m.readValue(respBody, String[].class);
		
		return super.getFileBlockLocations(file, start, len);
	}

	// delegate methods
	@Override
	public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2)
			throws IOException {
		return raw.append(lookup(arg0), arg1, arg2);
	}

	@Override
	public FSDataOutputStream create(Path arg0, FsPermission arg1,
			boolean arg2, int arg3, short arg4, long arg5, Progressable arg6)
			throws IOException {
		return raw.create(lookup(arg0), arg1, arg2, arg3, arg4, arg5, arg6);
	}

	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public boolean delete(Path arg0) throws IOException {
		return raw.delete(lookup(arg0));
	}

	@Override
	public boolean delete(Path arg0, boolean arg1) throws IOException {
		return raw.delete(lookup(arg0), arg1);
	}

	@Override
	public FileStatus getFileStatus(Path arg0) throws IOException {
		return dereference(raw.getFileStatus(lookup(arg0)));
	}

	@Override
	public Path getWorkingDirectory() {
		return workingDir;
	}

	@Override
	public FileStatus[] listStatus(Path arg0) throws IOException {

		FileStatus[] rawFiles = raw.listStatus(lookup(arg0));
		FileStatus[] ret = new FileStatus[rawFiles.length];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = dereference(rawFiles[i]);
		}
		return ret;
	}

	@Override
	public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
		return raw.mkdirs(lookup(arg0), arg1);
	}

	@Override
	public FSDataInputStream open(Path arg0, int arg1) throws IOException {
		return raw.open(lookup(arg0), arg1);
	}

	@Override
	public boolean rename(Path arg0, Path arg1) throws IOException {
		return raw.rename(lookup(arg0), lookup(arg1));
	}

	@Override
	public void setWorkingDirectory(Path arg0) {
		this.workingDir = arg0;
	}

}
