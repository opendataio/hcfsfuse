package alluxio.jnifuse;

import hcfsfuse.jnifuse.AbstractFuseFileSystem;

public class LibFuse {

  public native int fuse_main_real(AbstractFuseFileSystem fs, int argc, String[] argv);
}
