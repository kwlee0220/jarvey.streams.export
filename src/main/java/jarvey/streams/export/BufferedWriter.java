package jarvey.streams.export;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.utils.Bytes;

import com.vlkan.rfos.RotatingFileOutputStream;

import utils.func.Unchecked;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
final class BufferedWriter {
	private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
	
	private final ByteArrayOutputStream m_baos;
	private final RotatingFileOutputStream m_rfos;
	private long m_lastOffset = 0;
	
	BufferedWriter(RotatingFileOutputStream rfos, int bufSize) {
		m_rfos = rfos;
		m_baos = new ByteArrayOutputStream(bufSize);
	}
	
	void write(ConsumerRecord<Bytes,Bytes> record) {
		try {
			m_baos.write(record.value().get());
			m_baos.write(NEWLINE);
			m_lastOffset = record.offset();
		}
		catch ( IOException ignored ) { }
	}
	
	void flush() throws IOException {
		if ( m_baos.size() > 0 ) {
			m_rfos.write(m_baos.toByteArray());
			m_rfos.flush();
			m_baos.reset();
		}
	}
	
	OffsetAndMetadata getOffset() {
		return new OffsetAndMetadata(m_lastOffset + 1);
	}
	
	void close() throws IOException {
		Unchecked.runOrIgnore(m_baos::close);
		m_rfos.close();
	}
}