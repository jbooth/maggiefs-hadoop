package org.maggiefs.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.ArrayList;
import java.util.List;

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
	private String mountPrefix;
	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("mfs-default.xml");
	}

	public URI getUri() {
		return name;
	}

	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		setConf(conf);
		this.raw = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();
		this.name = uri;
		this.mountPrefix = conf.get("fs.mfs.mountPrefix");
		LOG.info("Initialized MaggieFS with URI " + uri.toString() + ", mountPrefix: " + this.mountPrefix);
		this.setWorkingDirectory(getHomeDirectory());
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
			long len) throws IOException {
		System.out.println("Looking up block locations for " + file.getPath());
		// resolve symlinks
		File absFile = lookupFile(file.getPath());
		List<BlockLocation> ret = new ArrayList<BlockLocation>();
		// get xattr
		UserDefinedFileAttributeView attrs = Files.getFileAttributeView(
				absFile.toPath(), UserDefinedFileAttributeView.class);
		ByteBuffer buff = ByteBuffer.allocate(64 * 1024);
		attrs.read("mfs.blockLocs", buff);
		if (buff.remaining() == 0) {
			return new BlockLocation[0];
		}
		buff.flip();
		byte[] destBytes = new byte[buff.remaining()];
		buff.get(destBytes);
		String blockLocStr = new String(destBytes);
		String[] lines = blockLocStr.split("\\n");
		for (String line : lines) {
			String[] elems = line.split("\\t");
			long startPos = Integer.parseInt(elems[0]);
			long endPos = Integer.parseInt(elems[1]);
			String[] hosts = elems[2].split(",");
			if ((startPos <= start && endPos > start)
					|| (startPos < (start + len) && endPos > start)) {
				ret.add(new BlockLocation(hosts, hosts, startPos,
						(endPos - startPos)));
			}
		}
		return ret.toArray(new BlockLocation[ret.size()]);
	}

	/**
	 * Adds the working dir in front of the path if necessary, changes scheme to
	 * file
	 */
	private Path lookup(Path path) {
		// handle working dir
		if (!path.isAbsolute()) {
			path = new Path(workingDir, path);
		}
		String p = path.toUri().getPath();
		if (this.mountPrefix != null && this.mountPrefix.length() > 0) {
			p = this.mountPrefix + p;
		}
		return new Path("file", null, p);
	}

	private File lookupFile(Path path) {
		path = lookup(path);
		return new File(path.toUri().getPath());
	}

	/* ensures scheme is mfs, pops off mountpoint if necessary */
	private FileStatus dereference(FileStatus s) {
		return new FileStatus(s.getLen(), s.isDir(), s.getReplication(),
				s.getBlockSize(), s.getModificationTime(), s.getAccessTime(),
				s.getPermission(), s.getOwner(), s.getGroup(),
				dereference(s.getPath()));
	}

	/* ensures scheme is mfs, pops off mountpoint if necessary */
	private Path dereference(Path p) {
		String path = p.toUri().getPath();
		if (this.mountPrefix != null && this.mountPrefix.length() > 0 && path.startsWith(mountPrefix)) {
			path = path.replace(this.mountPrefix, "/");
		}
		Path ret = new Path("mfs", "", path);
		return ret;
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
