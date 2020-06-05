package net.mbl.hcfsfuse;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

/**
 * Convenience class to pass around Alluxio-FUSE options.
 */
@AllArgsConstructor
@Getter
public class FuseOptions {

  private final String mountPoint;
  private final String root;
  private final boolean debug;
  private final List<String> fuseOpts;
  private final String[] confPaths;
}
