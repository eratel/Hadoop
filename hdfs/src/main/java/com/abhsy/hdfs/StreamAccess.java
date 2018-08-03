package com.abhsy.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * 相对那些封装好的方法而言的更底层一些的操作方式
 * 上层那些mapreduce   spark等运算框架，去hdfs中获取数据的时候，就是调的这种底层的api
 * @author
 *
 */
public class StreamAccess {
	
	FileSystem fs = null;

	@Before
	public void init() throws Exception {

		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "root");
		conf.set("fs.defaultFS", "hdfs://hadoop1:9000");
		fs = FileSystem.get(conf);
//		fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "hadoop");

	}
	
	
	
	@Test
	public void testDownLoadFileToLocal() throws IllegalArgumentException, IOException{
		
		//先获取一个文件的输入流----针对hdfs上的
		FSDataInputStream in = fs.open(new Path("/jdk-7u65-linux-i586.tar.gz"));
		
		//再构造一个文件的输出流----针对本地的
		FileOutputStream out = new FileOutputStream(new File("c:/jdk.tar.gz"));
		
		//再将输入流中数据传输到输出流
		IOUtils.copyBytes(in, out, 4096);
		
		
	}
	
	@Test
	public void testUploadByStream() throws Exception{
		
		//hdfs文件的输出流
		FSDataOutputStream fsout = fs.create(new Path("/aaa.txt"));
		
		//本地文件的输入流
		FileInputStream fsin = new FileInputStream("c:/111.txt");
		
		IOUtils.copyBytes(fsin, fsout,4096);
		
		
	}
	
	
	
	
	/**
	 * hdfs支持随机定位进行文件读取，而且可以方便地读取指定长度
	 * 用于上层分布式运算框架并发处理数据
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRandomAccess() throws IllegalArgumentException, IOException{
		//先获取一个文件的输入流----针对hdfs上的
		FSDataInputStream in = fs.open(new Path("/iloveyou.txt"));
		
		
		//可以将流的起始偏移量进行自定义
		in.seek(22);
		
		//再构造一个文件的输出流----针对本地的
		FileOutputStream out = new FileOutputStream(new File("d:/iloveyou.line.2.txt"));
		
		IOUtils.copyBytes(in,out,19L,true);
		
	}
	
	
	
	/**
	 * 读取指定的block
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testCat() throws IllegalArgumentException, IOException{
		
		FSDataInputStream in = fs.open(new Path("/weblog/input/access.log.10"));
		//拿到文件信息
		FileStatus[] listStatus = fs.listStatus(new Path("/weblog/input/access.log.10"));
		//获取这个文件的所有block的信息
		BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(listStatus[0], 0L, listStatus[0].getLen());
		
		
		//第一个block的长度
		long length = fileBlockLocations[0].getLength();
		//第一个block的起始偏移量
		long offset = fileBlockLocations[0].getOffset();

		System.out.println(length);
		System.out.println(offset);

		//获取第一个block写入输出流
//		IOUtils.copyBytes(in, System.out, (int)length);
		byte[] b = new byte[4096];
		
		FileOutputStream os = new FileOutputStream(new File("d:/block0"));
		while(in.read(offset, b, 0, 4096)!=-1){
			os.write(b);
			offset += 4096;
			if(offset>length) return;
		};
		
		os.flush();
		os.close();
		in.close();
		
		
	}
	
	

}
