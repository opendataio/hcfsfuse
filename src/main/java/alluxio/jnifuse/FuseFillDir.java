package alluxio.jnifuse;

import hcfsfuse.jnifuse.struct.FileStat;

import java.nio.ByteBuffer;

public class FuseFillDir {
  long address;

  FuseFillDir(long address) {
    this.address = address;
  }

  public native int fill(long address, long bufaddr, String name, ByteBuffer stbuf, long off);

  public int apply(long bufaddr, String name, FileStat stbuf, long off) {
    if (stbuf != null) {
      return fill(address, bufaddr, name, stbuf.buffer, off);
    } else {
      return fill(address, bufaddr, name, null, off);
    }
  }

  static {
    System.loadLibrary("jnifuse");
  }
}
