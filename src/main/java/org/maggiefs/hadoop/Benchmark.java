package org.maggiefs.hadoop;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Computes streaming write, streaming read, random read benchmarks
 * 
 * CL signature
 *
 */
public class Benchmark extends Configured implements Tool {
	
	/**
	 * Expectes a fully qualified pathname, eg hdfs://master:8020/workDir or mfs://localhost:1103/workDir
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new Benchmark(), args);
		System.exit(exit);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String op = args[0];
		Path workFile = new Path(args[1]);
		FileSystem fs = workFile.getFileSystem(getConf());

		System.out.println("Running " + args[0] + " benchmark for filesystem: " + fs.getName() + " : " + fs.getUri());
		if ("write".equals(op)) {
			int numGigs = Integer.parseInt(args[2]);
			doWrite(fs, workFile, numGigs);
		} else if ("scan".equals(op)) {
			doScan(fs, workFile);
		} else if ("rread".equals(op)) {
			doRread(fs, workFile);
		} else {
			System.out.println("Invalid args!  Usage:  benchmark <write|scan|rread> </path/to/workfile> [numGigs]");
		}
		
		return 0;
	}
	
	private void doWrite(FileSystem fs, Path workFile, int numGigs) throws IOException {
		long fileLength = numGigs * 1024 * 1024 * 1024;
		int numMegs = (int)(fileLength / (1024*1024));
		byte[] data = new byte[1024*1024];
		// make one big file
		if (fs.exists(workFile)) {
			fs.delete(workFile, false);
		}
		long startWrite = System.currentTimeMillis();
		OutputStream out = fs.create(workFile);
		try {
			for (int i = 0 ; i < numMegs ; i++) {
				out.write(data);
			}
		} finally {
			out.close();
		}
		long writeTime = System.currentTimeMillis() - startWrite;
		System.out.println("Time to write " + fileLength + " bytes: " + writeTime + " ms.");
		System.out.println("MB/s: " + (numMegs / (writeTime / 1000)));
	}
	
	private void doScan(FileSystem fs, Path workFile) throws IOException {
		long fileLength = fs.getFileStatus(workFile).getLen();
		int numMegs = (int)(fileLength / (1024*1024));
		// scan it
		long startRead = System.currentTimeMillis();
		byte[] buff = new byte[4096];
		FSDataInputStream in = fs.open(workFile);
		try {
			in.seek(0);
			while (in.read(buff) != -1) {
			}
			long scanTime = System.currentTimeMillis() - startRead;

			System.out.println("Time to read " + fileLength + " bytes: " + scanTime + " ms.");
			System.out.println("MB/s: " + (numMegs / ((double)scanTime / 1000.0)));
		} finally {
			in.close();
		}
	}
	
	private void doRread(FileSystem fs, Path workFile) throws IOException {
		long fileLength = fs.getFileStatus(workFile).getLen();
		byte[] buff = new byte[4096];
		FSDataInputStream in = fs.open(workFile);
		try {

			// read it in a series of 4kb reads in reverse so we don't trigger readahead.  'random'.
			long startRRead = System.currentTimeMillis();
			for (long pos = fileLength - 4096 ; pos >= 0 ; pos -= 4096) {
				in.readFully(pos, buff);
			}
			long rreadTime = System.currentTimeMillis() - startRRead;
			int numReads = (int)(fileLength / 4096);
			System.out.println("Time to perform " + numReads + " random reads: " + rreadTime);
			double msRead = rreadTime / numReads;
			System.out.println("Average ms / read: " + msRead);
		} finally {
			in.close();
		}
	}
	
}
