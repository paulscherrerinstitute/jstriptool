package ch.psi.bsread.message;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;

public class ValueTest {

	@Test
	public void testSerialization() throws Exception {
		ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
		int intVal = Integer.MAX_VALUE / 3;
		buf.asIntBuffer().put(intVal);
		Value<ByteBuffer> val = new ValueImpl<>(buf, new Timestamp(10, 2));

		Value<ByteBuffer> copy = SerializationHelper.copy(val);
		assertEquals(val.getTimestamp().getSec(), copy.getTimestamp().getSec());
		assertEquals(val.getTimestamp().getNs(), copy.getTimestamp().getNs());
		assertEquals(val.getValue().isDirect(), copy.getValue().isDirect());
		assertEquals(val.getValue().order(), copy.getValue().order());
		assertEquals(val.getValue().position(), copy.getValue().position());
		assertEquals(val.getValue().limit(), copy.getValue().limit());
		assertEquals(intVal, copy.getValue().asIntBuffer().get());

		buf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
		intVal = Integer.MAX_VALUE / 5000;
		buf.asIntBuffer().put(intVal);
		val = new ValueImpl<>(buf, new Timestamp(Long.MAX_VALUE / 2000, 459));

		copy = SerializationHelper.copy(val);
		assertEquals(val.getTimestamp().getSec(), copy.getTimestamp().getSec());
		assertEquals(val.getTimestamp().getNs(), copy.getTimestamp().getNs());
		assertEquals(val.getValue().isDirect(), copy.getValue().isDirect());
		assertEquals(val.getValue().order(), copy.getValue().order());
		assertEquals(val.getValue().position(), copy.getValue().position());
		assertEquals(val.getValue().limit(), copy.getValue().limit());
		assertEquals(intVal, copy.getValue().asIntBuffer().get());

		buf = ByteBuffer.allocateDirect(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
		intVal = Integer.MAX_VALUE / 9000;
		buf.asIntBuffer().put(intVal);
		val = new ValueImpl<>(buf, new Timestamp(Long.MAX_VALUE / 5000, 459));

		copy = SerializationHelper.copy(val);
		assertEquals(val.getTimestamp().getSec(), copy.getTimestamp().getSec());
		assertEquals(val.getTimestamp().getNs(), copy.getTimestamp().getNs());
		assertEquals(val.getValue().isDirect(), copy.getValue().isDirect());
		assertEquals(val.getValue().order(), copy.getValue().order());
		assertEquals(val.getValue().position(), copy.getValue().position());
		assertEquals(val.getValue().limit(), copy.getValue().limit());
		assertEquals(intVal, copy.getValue().asIntBuffer().get());

		buf = ByteBuffer.allocateDirect(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
		intVal = Integer.MAX_VALUE / 2000;
		buf.asIntBuffer().put(intVal);
		val = new ValueImpl<>(buf, new Timestamp(Long.MAX_VALUE / 3000, 459));

		copy = SerializationHelper.copy(val);
		assertEquals(val.getTimestamp().getSec(), copy.getTimestamp().getSec());
		assertEquals(val.getTimestamp().getNs(), copy.getTimestamp().getNs());
		assertEquals(val.getValue().isDirect(), copy.getValue().isDirect());
		assertEquals(val.getValue().order(), copy.getValue().order());
		assertEquals(val.getValue().position(), copy.getValue().position());
		assertEquals(val.getValue().limit(), copy.getValue().limit());
		assertEquals(intVal, copy.getValue().asIntBuffer().get());
	}

}
