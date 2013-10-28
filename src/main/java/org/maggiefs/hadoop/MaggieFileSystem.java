package org.maggiefs.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Wrapper for maggiefs for use by Hadoop.
 * 
 * fs.default.name should be of the form
 * mfs://localhost:localWebPort/localMountPoint. This allows us to avoid any
 * further configuration, by either delegating to to our local mount for all FS
 * operations.
 * 
 */
public class MaggieFileSystem extends FileSystem {
	static Log LOG = LogFactory.getLog(MaggieFileSystem.class);
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
			path = new Path(workingDir, path);
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
		this.name = uri;
		this.peerWebPort = uri.getPort();
		// this.mountPath = new Path("file", null, uri.getPath());
		// get mountPath from local service endpoint
		GetMethod get = new GetMethod("http://localhost:" + this.peerWebPort + "/mountPoint");
		try {
			int status = httpClient.executeMethod(get);
			if (status >= 300) {
				throw new IOException("Received HTTP status " + status
						+ " from remote endpoint while asking for /mountPoint!");
			}
			this.mountPathStr = new String(get.getResponseBody()).trim();
		} finally {
			get.releaseConnection();
		}
		LOG.info("Initialized MaggieFS with URI " + uri.toString() + " and local mount point " + this.mountPathStr);
		this.setWorkingDirectory(getHomeDirectory());
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
			long len) throws IOException {
		System.out.println("Looking up block locations for " + file.getPath());
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
		// pop off leading slashes
		mountRelFile = mountRelFile.replaceAll("^\\/*", "");
		String url = "http://localhost:" + peerWebPort + "/blockLocations?"
				+ "file=" + mountRelFile + "&start=" + start + "&length=" + len;
		System.out.println("Getting block locations with url " + url);
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
		return new FSDataOutputStream(new SyncableFileOutputStream(f,
				bufferSize, true), statistics);
	}

	/** {@inheritDoc} */
	@Override
	public FSDataOutputStream create(Path p, boolean overwrite, int bufferSize,
			short replication, long blockSize, Progressable progress)
			throws IOException {
		return create(p, overwrite, true, bufferSize, replication, blockSize,
				progress);
	}

	/** {@inheritDoc} */
	@Override
	public FSDataOutputStream create(Path p, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		FSDataOutputStream out = create(p, overwrite, bufferSize, replication,
				blockSize, progress);
		setPermission(p, permission);
		return out;
	}

	/** {@inheritDoc} */
	@Override
	public FSDataOutputStream createNonRecursive(Path p,
			FsPermission permission, boolean overwrite, int bufferSize,
			short replication, long blockSize, Progressable progress)
			throws IOException {
		FSDataOutputStream out = create(p, overwrite, false, bufferSize,
				replication, blockSize, progress);
		setPermission(p, permission);
		return out;
	}

	private FSDataOutputStream create(Path p, boolean overwrite,
			boolean createParent, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		File f = lookupFile(p);
		if (f.exists() && !overwrite) {
			throw new IOException("File already exists:" + f);
		}
		Path parent = p.getParent();
		if (parent != null) {
			if (!exists(parent)) {
				if (!createParent && !exists(parent)) {
					throw new FileNotFoundException(
							"Parent directory doesn't exist: " + parent);
				} else if (!mkdirs(parent)) {
					throw new IOException("Mkdirs failed to create " + parent);
				}
			}
		}
		FSDataOutputStream out = new FSDataOutputStream(
				new SyncableFileOutputStream(f, bufferSize, false), statistics);
		return out;
	}

	private static final long DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024 * 1024;

	@Deprecated
	public long getDefaultBlockSize() {
		return DEFAULT_BLOCK_SIZE;
	}

	public long getDefaultBlockSize(Path f) {
		return DEFAULT_BLOCK_SIZE;
	}

	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public boolean delete(Path arg0) throws IOException {
		return raw.delete(lookup(arg0));
	}

	@Override
	public boolean delete(Path p, boolean recursive) throws IOException {
		return raw.delete(lookup(p), recursive);
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
		if (rawFiles == null) {
			return new FileStatus[0];
		}
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
