package ch.psi.bsread.common.helper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntFunction;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import ch.psi.bsread.common.allocator.ByteArrayAllocator;
import ch.psi.bsread.common.allocator.ByteBufferAllocator;
import ch.psi.bsread.compression.Compressor;

/**
 * Copy of ch.psi.daq.common.helper.ByteBufferHelper
 */
public class ByteBufferHelper {
   private static final byte NULL_POSITION = 0;
   private static final byte DIRECT_POSITION = 1;
   private static final byte ORDER_POSITION = 2;
   private static final byte COMPRESS_POSITION = 3;
   private static final ByteArrayAllocator TMP_SERIALIZATION_ALLOCATOR = new ByteArrayAllocator();

   public static void write(ByteBuffer buffer, OutputStream os) throws IOException {
      ByteBufferHelper.write(buffer, os, buffer.remaining() > Compressor.DEFAULT_COMPRESS_THRESHOLD);
   }

   public static void write(ByteBuffer buffer, OutputStream os, boolean compress) throws IOException {
      if (buffer == null) {
         byte identifier = 0;
         identifier = ByteBufferHelper.setPosition(identifier, NULL_POSITION);
         // Important: writes one byte
         os.write(identifier);
      } else {
         byte identifier = 0;
         if (buffer.isDirect()) {
            identifier = ByteBufferHelper.setPosition(identifier, DIRECT_POSITION);
         }
         if (ByteOrder.LITTLE_ENDIAN.equals(buffer.order())) {
            identifier = ByteBufferHelper.setPosition(identifier, ORDER_POSITION);
         }
         if (compress) {
            identifier = ByteBufferHelper.setPosition(identifier, COMPRESS_POSITION);
         }
         // Important: writes one byte
         os.write(identifier);

         // write size
         ByteBuffer sizeBuf = ByteBufferAllocator.DEFAULT_ALLOCATOR.allocate(Integer.BYTES);
         sizeBuf.order(buffer.order());
         sizeBuf.putInt(0, buffer.remaining());
         os.write(sizeBuf.array());

         if (compress) {
            LZ4BlockOutputStream lzos = new LZ4BlockOutputStream(os, buffer.remaining());

            writeByteBuffer(buffer, lzos);

            lzos.finish();
         } else {
            writeByteBuffer(buffer, os);
         }
      }
   }

   public static void write(ByteBuffer buffer, DataOutput os) throws IOException {
      if (buffer == null) {
         byte identifier = 0;
         identifier = ByteBufferHelper.setPosition(identifier, NULL_POSITION);
         // Important: writes one byte
         os.write(identifier);
      } else {
         byte identifier = 0;
         if (buffer.isDirect()) {
            identifier = ByteBufferHelper.setPosition(identifier, DIRECT_POSITION);
         }
         if (ByteOrder.LITTLE_ENDIAN.equals(buffer.order())) {
            identifier = ByteBufferHelper.setPosition(identifier, ORDER_POSITION);
         }
         // Important: writes one byte
         os.write(identifier);

         // write size
         ByteBuffer sizeBuf = ByteBufferAllocator.DEFAULT_ALLOCATOR.allocate(Integer.BYTES);
         sizeBuf.order(buffer.order());
         sizeBuf.putInt(0, buffer.remaining());
         os.write(sizeBuf.array());

         writeByteBuffer(buffer, os);
      }
   }

   public static void writeByteBuffer(ByteBuffer buffer, OutputStream os) throws IOException {
      if (buffer.hasArray()) {
         os.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
      } else {
         byte[] bytes = TMP_SERIALIZATION_ALLOCATOR.apply(buffer.remaining());
         // bulk methods are way faster than reading/writing single bytes
         buffer.duplicate().order(buffer.order()).get(bytes, 0, buffer.remaining());
         os.write(bytes, 0, buffer.remaining());
      }
   }

   public static void writeByteBuffer(ByteBuffer buffer, DataOutput os) throws IOException {
      if (buffer.hasArray()) {
         os.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
      } else {
         byte[] bytes = TMP_SERIALIZATION_ALLOCATOR.apply(buffer.remaining());
         // bulk methods are way faster than reading/writing single bytes
         buffer.duplicate().order(buffer.order()).get(bytes, 0, buffer.remaining());
         os.write(bytes, 0, buffer.remaining());
      }
   }

   public static byte[] toArray(ByteBuffer buffer, IntFunction<byte[]> allocator) {
      if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.remaining() == buffer.array().length) {
         return buffer.array();
      } else {
         return toArrayCopy(buffer, allocator);
      }
   }

   public static byte[] toArrayCopy(ByteBuffer buffer, IntFunction<byte[]> allocator) {
      byte[] bytes = allocator.apply(buffer.remaining());
      // bulk methods are way faster than reading/writing single bytes
      buffer.duplicate().order(buffer.order()).get(bytes, 0, buffer.remaining());
      return bytes;
   }

   public static ByteBuffer read(InputStream is) throws IOException {
      // Important: reads one byte
      byte identifier = (byte) is.read();
      if (identifier == -1 || ByteBufferHelper.isPositionSet(identifier, NULL_POSITION)) {
         return null;
      }

      ByteOrder byteOrder =
            ByteBufferHelper.isPositionSet(identifier, ORDER_POSITION) ? ByteOrder.LITTLE_ENDIAN
                  : ByteOrder.BIG_ENDIAN;

      ByteBuffer sizeBuf = ByteBufferAllocator.DEFAULT_ALLOCATOR.allocate(Integer.BYTES);
      sizeBuf.order(byteOrder);
      is.read(sizeBuf.array());
      int size = sizeBuf.getInt(0);

      if (ByteBufferHelper.isPositionSet(identifier, COMPRESS_POSITION)) {
         LZ4BlockInputStream lzis = new LZ4BlockInputStream(is);

         return readByteBuffer(identifier, lzis, size, byteOrder);
      } else {
         return readByteBuffer(identifier, is, size, byteOrder);
      }
   }

   public static ByteBuffer read(DataInput is) throws IOException {
      byte identifier = is.readByte();
      if (identifier == -1 || ByteBufferHelper.isPositionSet(identifier, NULL_POSITION)) {
         return null;
      }

      ByteOrder byteOrder =
            ByteBufferHelper.isPositionSet(identifier, ORDER_POSITION) ? ByteOrder.LITTLE_ENDIAN
                  : ByteOrder.BIG_ENDIAN;

      int size = is.readInt();
      if (ByteOrder.LITTLE_ENDIAN.equals(byteOrder)) {
         size = Integer.reverseBytes(size);
      }

      return readByteBuffer(identifier, is, size, byteOrder);
   }

   public static void readByteBuffer(InputStream is, ByteBuffer buffer) throws IOException {
      int size = buffer.remaining();
      byte[] valBytes;
      if (!buffer.hasArray()) {
         valBytes = TMP_SERIALIZATION_ALLOCATOR.apply(size);
      } else {
         valBytes = buffer.array();
      }

      int len = size;
      int off = buffer.position();
      int read;
      while ((read = is.read(valBytes, off, len)) > 0) {
         off += read;
         len -= read;
      }

      if (!buffer.hasArray()) {
         // bulk methods are way faster than reading/writing single bytes
         buffer.put(valBytes, 0, off);
         // make ready for read
         buffer.flip();
      }
   }

   private static ByteBuffer readByteBuffer(byte identifier, InputStream is, int size, ByteOrder byteOrder)
         throws IOException {
      boolean isDirect = ByteBufferHelper.isPositionSet(identifier, DIRECT_POSITION);

      ByteBuffer buffer;
      if (isDirect) {
         buffer = ByteBufferAllocator.DEFAULT_ALLOCATOR.allocateDirect(size);
      } else {
         buffer = ByteBufferAllocator.DEFAULT_ALLOCATOR.allocateHeap(size);
      }
      buffer.order(byteOrder);

      byte[] valBytes;
      if (!buffer.hasArray()) {
         valBytes = TMP_SERIALIZATION_ALLOCATOR.apply(size);
      } else {
         valBytes = buffer.array();
      }

      int len = size;
      int off = 0;
      int read;
      while ((read = is.read(valBytes, off, len)) > 0) {
         off += read;
         len -= read;
      }

      if (!buffer.hasArray()) {
         // bulk methods are way faster than reading/writing single bytes
         buffer.put(valBytes, 0, off);
         // make ready for read
         buffer.flip();
      }

      return buffer;
   }

   private static ByteBuffer readByteBuffer(byte identifier, DataInput is, int size, ByteOrder byteOrder)
         throws IOException {
      boolean isDirect = ByteBufferHelper.isPositionSet(identifier, DIRECT_POSITION);

      ByteBuffer buffer;
      if (isDirect) {
         buffer = ByteBufferAllocator.DEFAULT_ALLOCATOR.allocateDirect(size);
      } else {
         buffer = ByteBufferAllocator.DEFAULT_ALLOCATOR.allocateHeap(size);
      }
      buffer.order(byteOrder);

      byte[] valBytes;
      if (!buffer.hasArray()) {
         valBytes = TMP_SERIALIZATION_ALLOCATOR.apply(size);
      } else {
         valBytes = buffer.array();
      }

      is.readFully(valBytes, 0, size);

      if (!buffer.hasArray()) {
         // bulk methods are way faster than reading/writing single bytes
         buffer.put(valBytes, 0, size);
         // make ready for read
         buffer.flip();
      }

      return buffer;
   }

   /**
    * Sets the bit of a specific position.
    * 
    * @param descriptor The initial descriptor
    * @param position The position to set
    * @return byte The modified descriptor
    */
   public static byte setPosition(byte descriptor, int position) {
      if (position > Byte.SIZE - 1) {
         throw new IllegalStateException("Position must be smaller than number of bits of byte.");
      }

      return descriptor |= (1 << position);
   }

   /**
    * Sets the bit of a specific position.
    * 
    * @param descriptor The initial descriptor
    * @param position The position to set
    * @param state The state
    * @return byte The modified descriptor
    */
   public static byte setPosition(byte descriptor, int position, boolean state) {
      if (state) {
         return setPosition(descriptor, position);
      } else {
         return descriptor;
      }
   }

   /**
    * Sets the bit of a specific position.
    * 
    * @param descriptor The initial descriptor
    * @param position The position to set
    * @return short The modified descriptor
    */
   public static short setPosition(short descriptor, int position) {
      if (position > Short.SIZE - 1) {
         throw new IllegalStateException("Position must be smaller than number of bits of short.");
      }

      return descriptor |= (1 << position);
   }

   /**
    * Sets the bit of a specific position.
    * 
    * @param descriptor The initial descriptor
    * @param position The position to set
    * @param state The state
    * @return short The modified descriptor
    */
   public static short setPosition(short descriptor, int position, boolean state) {
      if (state) {
         return setPosition(descriptor, position);
      } else {
         return descriptor;
      }
   }

   /**
    * Sets the bit of a specific position.
    * 
    * @param descriptor The initial descriptor
    * @param position The position to set
    * @return int The modified descriptor
    */
   public static int setPosition(int descriptor, int position) {
      if (position > Integer.SIZE - 1) {
         throw new IllegalStateException("Position must be smaller than number of bits of int.");
      }

      return descriptor |= (1 << position);
   }

   /**
    * Sets the bit of a specific position.
    * 
    * @param descriptor The initial descriptor
    * @param position The position to set
    * @param state The state
    * @return int The modified descriptor
    */
   public static int setPosition(int descriptor, int position, boolean state) {
      if (state) {
         return setPosition(descriptor, position);
      } else {
         return descriptor;
      }
   }

   /**
    * Sets the bit of a specific position.
    * 
    * @param descriptor The initial descriptor
    * @param position The position to set
    * @return long The modified descriptor
    */
   public static long setPosition(long descriptor, int position) {
      if (position > Long.SIZE - 1) {
         throw new IllegalStateException("Position must be smaller than number of bits of long.");
      }

      return descriptor |= (1 << position);
   }

   /**
    * Sets the bit of a specific position.
    * 
    * @param descriptor The initial descriptor
    * @param position The position to set
    * @param state The state
    * @return long The modified descriptor
    */
   public static long setPosition(long descriptor, int position, boolean state) {
      if (state) {
         return setPosition(descriptor, position);
      } else {
         return descriptor;
      }
   }

   /**
    * Determines if the bit of a specific position is set.
    * 
    * @param descriptor The descriptor
    * @param position The position
    * @return boolean true if the bit is set, false otherwise
    */
   public static boolean isPositionSet(byte descriptor, int position) {
      return position < Byte.SIZE && (descriptor & (1 << position)) != 0;
   }

   /**
    * Determines if the bit of a specific position is set.
    * 
    * @param descriptor The descriptor
    * @param position The position
    * @return boolean true if the bit is set, false otherwise
    */
   public static boolean isPositionSet(short descriptor, int position) {
      return position < Short.SIZE && (descriptor & (1 << position)) != 0;
   }

   /**
    * Determines if the bit of a specific position is set.
    * 
    * @param descriptor The descriptor
    * @param position The position
    * @return boolean true if the bit is set, false otherwise
    */
   public static boolean isPositionSet(int descriptor, int position) {
      return position < Integer.SIZE && (descriptor & (1 << position)) != 0;
   }

   /**
    * Determines if the bit of a specific position is set.
    * 
    * @param descriptor The descriptor
    * @param position The position
    * @return boolean true if the bit is set, false otherwise
    */
   public static boolean isPositionSet(long descriptor, int position) {
      return position < Long.SIZE && (descriptor & (1 << position)) != 0;
   }

   /**
    * Copies a ByteBuffer into a newly allocated ByteBuffer
    * 
    * @param buffer The ByteBuffer to copy
    * @return ByteBuffer The copy
    */
   public static ByteBuffer copy(ByteBuffer buffer) {
      ByteBuffer copy;
      if (buffer.isDirect()) {
         copy = ByteBufferAllocator.DEFAULT_ALLOCATOR.allocateDirect(buffer.remaining());
      } else {
         copy = ByteBufferAllocator.DEFAULT_ALLOCATOR.allocateHeap(buffer.remaining());
      }

      copy.put(buffer.duplicate().order(buffer.order()));
      copy.flip();
      return copy;
   }

   /**
    * Converts the received ByteBuffer into a direct ByteBuffer (might return the same reference in
    * case buffer is already direct).
    * 
    * @param buffer The ByteBuffer
    * 
    * @return Buffer The direct ByteBuffer
    */
   public static ByteBuffer asDirect(ByteBuffer buffer) {
      if (buffer.isDirect()) {
         return buffer;
      } else {
         ByteBuffer direct = ByteBufferAllocator.DEFAULT_ALLOCATOR.allocateDirect(buffer.remaining());

         direct.order(buffer.order());
         direct.put(buffer.duplicate().order(buffer.order()));
         direct.flip();
         return direct;
      }
   }

   /**
    * Copies a ByteBuffer into a newly allocated byte array (does not use underlying byte array of
    * some ByteBuffer implementations)
    * 
    * @param buffer The ByteBuffer to copy
    * @return byte[] The copy
    */
   public static byte[] copyToByteArray(ByteBuffer buffer) {
      byte[] copy = new byte[buffer.remaining()];
      buffer.duplicate().order(buffer.order()).get(copy);
      return copy;
   }

   /**
    * Extracts a byte array that contains the content of the ByteBuffer (might reuse underlying byte
    * arrays of some ByteBuffer implementations).
    * 
    * @param buffer The ByteBuffer
    * @return byte[] The byte array
    */
   public static byte[] extractByteArray(ByteBuffer buffer) {
      if (buffer.hasArray() && buffer.position() == 0 && buffer.remaining() == buffer.capacity()) {
         return buffer.array();
      } else {
         return copyToByteArray(buffer);
      }
   }

   /**
    * Resets the buffers position and limit to its full capacity.
    * 
    * @param buffer The ByteBuffer
    */
   public static void resetBuffer(ByteBuffer buffer) {
      buffer.position(0);
      buffer.limit(buffer.capacity());
   }

   /**
    * Generates a string from a ByteBuffer.
    * 
    * @param buffer The ByteBuffer
    * @param delemiter The delemiter of the bytes of the ByteBuffer
    * @param header An optional header
    * @return String the String
    */
   public static String toString(ByteBuffer buffer, String delemiter, String header) {
      StringBuilder builder = new StringBuilder();

      if (header != null && !"".equals(header)) {
         builder.append(header).append("\n");
      }

      builder.append(buffer.position()).append(", ");
      builder.append(buffer.limit()).append(", ");
      builder.append(buffer.remaining()).append(", ");
      builder.append(buffer.order());
      builder.append("\n");

      for (int i = 0; i < buffer.remaining();) {
         builder.append(buffer.get(buffer.position() + i));
         ++i;
         if (i < buffer.remaining()) {
            builder.append(delemiter);
         }
      }

      return builder.toString();
   }
}
