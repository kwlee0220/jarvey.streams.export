package rhdfos;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

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
			HdfsFile file = HdfsFile.of(fs, "/tmp").getChild("xxx");
			try ( FSDataOutputStream fsdos = file.openOutputStream(false) ) {
				fsdos.writeUTF("abcdef\n");
			}
		}
	}
}
