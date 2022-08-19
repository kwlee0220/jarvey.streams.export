/**
 * 
 */
package rhdfos;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Objects;

import utils.io.FileProxy;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HdfsFile implements FileProxy {
	private final FileSystem m_fs;
	private final Path m_path;
	
	public static HdfsFile of(FileSystem fs, Path path) {
		return new HdfsFile(fs, path);
	}
	
	public static HdfsFile of(FileSystem fs, String path) {
		return new HdfsFile(fs, new Path(path));
	}
	
	private HdfsFile(FileSystem fs, Path path) {
		m_fs = fs;
		m_path = path;
	}
	
	public FileSystem getFileSystem() {
		return m_fs;
	}

	@Override
	public String getName() {
		return m_path.getName();
	}

	@Override
	public String getAbsolutePath() {
		return m_path.toString();
	}

	@Override
	public HdfsFile getParent() {
		Path parent = m_path.getParent();
		return parent != null ? HdfsFile.of(m_fs, parent) : null;
	}

	@Override
	public HdfsFile getChild(String childName) {
		return HdfsFile.of(m_fs, new Path(m_path, childName));
	}

	@Override
	public boolean isDirectory() {
		try {
			return m_fs.getFileStatus(m_path).isDirectory();
		}
		catch ( IOException e ) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public List<FileProxy> listFiles() throws IOException {
		return FStream.of(m_fs.listStatus(m_path))
						.map(s -> of(m_fs, s.getPath()))
						.cast(FileProxy.class)
						.toList();
	}

	@Override
	public long length() throws IOException {
		return m_fs.getFileStatus(m_path).getLen();
    }

	@Override
	public boolean exists() {
		try {
			return m_fs.exists(m_path);
		}
		catch ( IOException e ) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public boolean delete() {
		try {
			if ( !exists() ) {
				return true;
			}
			
			return m_fs.delete(m_path, true);
		}
		catch ( IOException e ) {
			return false;
		}
	}

	@Override
	public void renameTo(FileProxy dstFile, boolean replaceExisting) throws IOException {
		if ( !exists() ) {
			throw new IOException("source not found: file=" + m_path);
		}
		
		if ( !(dstFile instanceof HdfsFile) ) {
			throw new IllegalArgumentException("incompatible destination file handle: " + dstFile);
		}
		HdfsFile dst = (HdfsFile)dstFile;
		
		boolean dstExists = dst.exists();
		if ( replaceExisting && dstExists ) {
			throw new IOException("destination exists: file=" + dstFile);
		}

		HdfsFile parent = dst.getParent();
		if ( parent != null ) {
			if ( !parent.exists() ) {
				parent.mkdirs();
			}
			else if ( !parent.isDirectory() ) {
				throw new IOException("destination's parent is not a directory: path=" + parent);
			}
		}
		if ( !replaceExisting && dstExists ) {
			dst.delete();
		}
		
		if ( !m_fs.rename(m_path, dst.m_path) ) {
			throw new IOException("fails to rename to " + dst);
		}

		// 파일 이동 후, 디렉토리가 비게 되면 해당 디렉토리를 올라가면서 삭제한다.
		if ( parent != null ) {
			parent.deleteIfEmptyDirectory();
		}
	}

	@Override
	public boolean mkdirs() {
		if ( exists() ) {
			return false;
		}
		
		HdfsFile parent = getParent();
		if ( parent != null ) {
			if ( !parent.exists() ) {
				parent.mkdirs();
			}
			else if ( !parent.isDirectory() ) {
				throw new UncheckedIOException(new IOException("the parent file is not a directory"));
			}
		}
		
		try {
			return m_fs.mkdirs(m_path);
		}
		catch ( IOException e ) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public HdfsFile proxy(String path) {
		return HdfsFile.of(m_fs, new Path(path));
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		return m_fs.open(m_path);
	}

	@Override
	public FSDataOutputStream openOutputStream(boolean append) throws IOException {
		FileProxy parent = getParent();
		if ( parent != null ) {
			parent.mkdirs();
		}
		
		if ( append ) {
			return m_fs.create(m_path);
		}
		else {
			return m_fs.append(m_path);
		}
	}
	
	@Override
	public String toString() {
		return m_path.toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( this == null || getClass() != HdfsFile.class ) {
			return false;
		}
		
		HdfsFile other = (HdfsFile)obj;
		return Objects.equal(m_path, other.m_path);
	}
}
