package com.adaptris.kafka.embedded;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileCleaningTracker;
import org.apache.commons.io.FileDeleteStrategy;

class FileHelper {
  private static FileCleaningTracker cleaner = new FileCleaningTracker();

  private FileHelper() {}

  static File createTempDir(String dirPrefix, Object marker) {
    File f = null;
    try {
      f = File.createTempFile(dirPrefix, "");
      f.delete();
      f.mkdirs();
      f.deleteOnExit();
      cleaner.track(f, marker, FileDeleteStrategy.FORCE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return f;
  }
}
