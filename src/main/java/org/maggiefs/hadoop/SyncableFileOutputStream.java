package org.maggiefs.hadoop;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.Syncable;

/**
 * Wraps a BufferedOutputStream with the Syncable interface
 *
 */
public class SyncableFileOutputStream extends OutputStream implements Syncable {
	private final OutputStream out;
	private final FileDescriptor fd;
	
	public SyncableFileOutputStream(File f, int bufferSize, boolean append) throws IOException {
		FileOutputStream fileOut = new FileOutputStream(f, append);
		this.out = new BufferedOutputStream(fileOut,bufferSize);
		this.fd = fileOut.getFD();
	}

	@Override
	public void sync() throws IOException {
		flush();  // flush calls fsync for us
	}
	
	@Override
	public void close() throws IOException {
		flush(); // flush calls fsync for us
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
		fd.sync();
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
