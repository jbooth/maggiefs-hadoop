package org.maggiefs.hadoop;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.UserDefinedFileAttributeView;

public class TestXattr {

	public static void main(String[] args) throws Exception {
		File f = new File("/tmp/mfs/hi.txt");
		Path p = f.toPath();
		UserDefinedFileAttributeView attrs = Files.getFileAttributeView(p, UserDefinedFileAttributeView.class);
		System.out.println(attrs.list());
		ByteBuffer buff = ByteBuffer.allocate(4096);
		attrs.read("mfs.blockLocs", buff);
		buff.flip();
		byte[] destBytes = new byte[buff.remaining()];
		System.out.println("buff.remaining: " + buff.remaining());
		buff.get(destBytes);
		System.out.println("mfs.blockLocs: " + new String(destBytes));
	}
}
