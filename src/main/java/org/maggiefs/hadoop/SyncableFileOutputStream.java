package org.maggiefs.hadoop;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Syncable;

/**
 * Wraps a BufferedOutputStream with the Syncable interface
 *
 */
public class SyncableFileOutputStream extends OutputStream implements Syncable {
	private final OutputStream out;
	private final FileDescriptor fd;
	static Log LOG = LogFactory.getLog(SyncableFileOutputStream.class);
	
	public SyncableFileOutputStream(File f, int bufferSize, boolean append) throws IOException {
		FileOutputStream fileOut = new FileOutputStream(f, append);
		// whatever
		if (bufferSize < 65536) {
			bufferSize = 65536;
		}
		this.out = new BufferedOutputStream(fileOut,bufferSize);
		this.fd = fileOut.getFD();
	}

	@Override
	public void sync() throws IOException {
		out.flush();  
		// lol
//		int fdInt = sun.misc.SharedSecrets.getJavaIOFileDescriptorAccess().get(fd);
//		if (fdInt < 0) {
//			LOG.warn("Attempting to sync an invalid fd!  Fd:  " + fdInt)
//		}
		fd.sync();
	}
	
	@Override
	public void close() throws IOException {
		sync(); // sync calls fsync for us
		out.close();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() != SyncableFileOutputStream.class) return false;
		SyncableFileOutputStream other = (SyncableFileOutputStream)obj;
		return this.out.equals(other.out);
	}

	@Override
	public void flush() throws IOException {
		out.flush();
	}

	@Override
	public int hashCode() {
		return out.hashCode();
	}

	@Override
	public String toString() {
		return out.toString();
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		out.write(b, off, len);
	}

	@Override
	public void write(byte[] b) throws IOException {
		out.write(b);
	}

	@Override
	public void write(int b) throws IOException {
		out.write(b);
	}
}
