package rhdfos;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import jarvey.HdfsPath;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestHdfs {
	public static final void main(String... args) throws Exception {
		Configuration conf = new Configuration();
		
		File confFile = new File("hadoop-conf/jarvey-hdfs.xml");
		try ( InputStream is = new FileInputStream(confFile) ) {
			conf.addResource(is);
			
			FileSystem fs = FileSystem.get(conf);
			HdfsPath file = HdfsPath.of(fs, "yyy");
			try ( FSDataOutputStream fsdos = file.create(true) ) {
				fsdos.writeUTF("abcdef\n");
			}
		}
	}
}
