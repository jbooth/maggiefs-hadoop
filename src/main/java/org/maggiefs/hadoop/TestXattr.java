package org.maggiefs.hadoop;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.UserDefinedFileAttributeView;

public class TestXattr {

	public static void main(String[] args) throws Exception {
		File f = new File("/home/jay/ohmdb.txt");
		Path p = f.toPath();
		UserDefinedFileAttributeView attrs = Files.getFileAttributeView(p, UserDefinedFileAttributeView.class);
		System.out.println(attrs.list());
	}
}
