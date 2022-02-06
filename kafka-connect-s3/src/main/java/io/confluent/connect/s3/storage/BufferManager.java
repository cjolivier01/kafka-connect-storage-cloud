package io.confluent.connect.s3.storage;

import java.nio.ByteBuffer;

public interface BufferManager {

  ByteBuffer allocate();

  ByteBuffer release(ByteBuffer byteBuffer) throws IllegalArgumentException;

  int bufferSize();
}
