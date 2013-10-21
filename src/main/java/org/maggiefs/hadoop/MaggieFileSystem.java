package org.maggiefs.hadoop;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Wrapper for maggiefs for use by Hadoop.
 * 
 * fs.default.name should be of the form mfs://localhost:localWebPort/localMountPoint.
 * This allows us to avoid any further configuration, by either delegating to
 * to our local mount for all FS operations.
 * 
 */
public class MaggieFileSystem extends FileSystem {
	
	private URI name;
	private RawLocalFileSystem raw;
	private Path workingDir;
	private String mountPathStr;
	private int peerWebPort;
	private final HttpClient httpClient;
	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("mfs-default.xml");
	}

	public MaggieFileSystem() {
		MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
		this.httpClient = new HttpClient(connectionManager);
	}

	/** Adds the mountpoint in front of the path, changes scheme to file */
	private Path lookup(Path path) {
		// handle working dir
		if (!path.isAbsolute()) {
			path = new Path(workingDir,path);
		}
		// handle mountpoint
		String p = path.toUri().getPath();
		p = this.mountPathStr + p;
		Path ret = new Path("file", null, p);
		return ret;
	}

	private File lookupFile(Path path) {
		path = lookup(path);
		return new File(path.toUri().getPath());
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
		if (path.startsWith(mountPathStr)) {
			path = path.replace(mountPathStr, "/");
		}
		Path ret = new Path("mfs", "localhost:" + peerWebPort, path);
		return ret;
	}

	public URI getUri() {
		return name;
	}

	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		setConf(conf);
		this.raw = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();
		if (!uri.getPath().startsWith("/")) {
			throw new RuntimeException("Mountpoint must be absolute!");
		}
		try {
			this.name = new URI("mfs://localhost:" + uri.getPort() 	+ uri.getPath());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
		//this.mountPath = new Path("file", null, uri.getPath());
		this.mountPathStr = uri.getPath();
		this.peerWebPort = uri.getPort();
		this.setWorkingDirectory(getHomeDirectory());
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
			long len) throws IOException {
		// resolve symlinks
		Path fullSysPath = lookup(file.getPath());
		File absFile = new File(fullSysPath.toUri().getPath());
		File resolvedFile = absFile.getCanonicalFile();

		// throw error if not still in filesystem
		if (!resolvedFile.toString().startsWith(this.mountPathStr)) {
			throw new IOException("Symlink resolved to non mountpoint path "
					+ resolvedFile.toString());
		}
		// pop off mountpoint
		// json endpoint expects relative path from mountpoint
		String mountRelFile = resolvedFile.toString().substring(
				this.mountPathStr.length());
		String url = "http://localhost:" + peerWebPort + "/blockLocations?"
				+ "file=" + mountRelFile + "&start=" + start + "&length=" + len;
		// get block locations from json endpoint
		GetMethod get = new GetMethod(url);
		byte[] respBody = "[]".getBytes();
		try {
			int status = httpClient.executeMethod(get);
			if (status >= 300) {
				throw new IOException("Received HTTP status " + status
						+ " from remote endpoint!");
			}
			respBody = get.getResponseBody();
		} finally {
			get.releaseConnection();
		}
		// parse from json
		ObjectMapper m = new ObjectMapper();
		BlockLocation[] blocks = m.readValue(respBody, BlockLocation[].class);
		return blocks;
	}

	// delegate methods
	@Override
	public FSDataOutputStream append(Path path, int bufferSize, Progressable p)
			throws IOException {
		File f = lookupFile(path);
		if (f.exists()) {
			throw new FileNotFoundException("File " + f + " not found.");
		}
		if (f.isDirectory()) {
			throw new IOException("Cannot append to a diretory (=" + f + " ).");
		}
		return new FSDataOutputStream(new SyncableFileOutputStream(f, bufferSize,true), statistics);
	}

	@Override
	public FSDataOutputStream create(Path p,
		      FsPermission permission,
		      boolean overwrite,
		      int bufferSize,
		      short replication,
		      long blockSize,
		      Progressable progress)
			throws IOException {
		File f = lookupFile(p);
		if (f.exists() && !overwrite) {
			throw new IOException("File already exists:"+f);
		}
		Path parent = p.getParent();
	    if (parent != null) {
	      if (!exists(parent)) {
	        throw new FileNotFoundException("Parent directory doesn't exist: "
	            + parent);
	      }
	    }
		FSDataOutputStream out = new FSDataOutputStream(new SyncableFileOutputStream(f, bufferSize, false), statistics);
		FileUtil.setPermission(f, permission);
		return out;
	}

	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public boolean delete(Path arg0) throws IOException {
		return raw.delete(lookup(arg0));
	}

	@Override
	public boolean delete(Path p, boolean recursive) throws IOException {
		return raw.delete(lookup(p),recursive);
	}
	
	@Override
	public FileStatus getFileStatus(Path arg0) throws IOException {
		return dereference(raw.getFileStatus(lookup(arg0)));
	}

	@Override
	public Path getWorkingDirectory() {
		return this.workingDir;
	}

	@Override
	public void setWorkingDirectory(Path p) {
		this.workingDir = p;
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
}
