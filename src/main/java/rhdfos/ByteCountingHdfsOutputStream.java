package rhdfos;
/**
 * 
 */

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Syncable;

import com.vlkan.rfos.ByteCountingOutputStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ByteCountingHdfsOutputStream extends ByteCountingOutputStream
										implements DataOutput, Syncable, CanSetDropBehind {
	private final FSDataOutputStream m_fsdos;
	
	protected ByteCountingHdfsOutputStream(FSDataOutputStream parent, long size) {
		super(parent, size);
		
		m_fsdos = parent;
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		m_fsdos.writeBoolean(v);
	}

	@Override
	public void writeByte(int v) throws IOException {
		m_fsdos.writeByte(v);
	}

	@Override
	public void writeShort(int v) throws IOException {
		m_fsdos.writeShort(v);
	}

	@Override
	public void writeChar(int v) throws IOException {
		m_fsdos.writeChar(v);
	}

	@Override
	public void writeInt(int v) throws IOException {
		m_fsdos.writeInt(v);
	}

	@Override
	public void writeLong(long v) throws IOException {
		m_fsdos.writeLong(v);
	}

	@Override
	public void writeFloat(float v) throws IOException {
		m_fsdos.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) throws IOException {
		m_fsdos.writeDouble(v);
	}

	@Override
	public void writeBytes(String s) throws IOException {
		m_fsdos.writeBytes(s);
	}

	@Override
	public void writeChars(String s) throws IOException {
		m_fsdos.writeChars(s);
	}

	@Override
	public void writeUTF(String s) throws IOException {
		m_fsdos.writeUTF(s);
	}

	@Override
	public void sync() throws IOException {
		m_fsdos.sync();
	}

	@Override
	public void hflush() throws IOException {
		m_fsdos.hflush();
	}

	@Override
	public void hsync() throws IOException {
		m_fsdos.hsync();
	}

	@Override
	public void setDropBehind(Boolean dropCache) throws IOException, UnsupportedOperationException {
		m_fsdos.setDropBehind(dropCache);
	}
}
