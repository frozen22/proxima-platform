package cz.o2.proxima.direct.jdbc;

import cz.o2.proxima.direct.randomaccess.RandomOffset;

public class Offsets {
  public static class Raw implements RandomOffset {
    private final String key;
    public Raw(String key) {
      this.key = key;
    }
  }
}
