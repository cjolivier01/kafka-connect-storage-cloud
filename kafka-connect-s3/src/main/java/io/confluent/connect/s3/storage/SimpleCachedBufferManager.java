package io.confluent.connect.s3.storage;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple buffer cache which can be disabled to behave as none-cached
 */
public class SimpleCachedBufferManager implements BufferManager {
  ConcurrentHashMap.KeySetView<ByteBuffer, Boolean> bufferSet = ConcurrentHashMap.newKeySet();

  Lock lock = new ReentrantLock();
  final int bufferSize;
  final boolean enabled;

  public SimpleCachedBufferManager(int bufferSize, boolean enabled) {
    this.enabled = enabled;
    this.bufferSize = bufferSize;
  }

  @Override
  public ByteBuffer allocate() {
    lock.lock();
    try {
      ByteBuffer byteBuffer;
      Iterator<ByteBuffer> iterator = bufferSet.iterator();
      if (iterator.hasNext()) {
        byteBuffer = iterator.next();
        bufferSet.remove(byteBuffer);
      } else {
        byteBuffer = createNewBuffer();
      }
      return byteBuffer;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public ByteBuffer release(ByteBuffer byteBuffer) throws IllegalArgumentException {
    byteBuffer.clear();
    if (!enabled) {
      return byteBuffer;
    }
    lock.lock();
    try {
      if (bufferSet.contains(byteBuffer)) {
        throw new IllegalArgumentException(
          "Attempt to release a buffer that is already released."
        );
      }
      bufferSet.add(byteBuffer);
      return null;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int bufferSize() {
    return this.bufferSize;
  }

  private ByteBuffer createNewBuffer() {
    return ByteBuffer.allocate(bufferSize);
  }
}
