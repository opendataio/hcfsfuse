package hcfsfuse.fuse;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.FuseException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Main entry point to HCFS-FUSE.
 */
public class HCFSFuse {
  public static final Logger LOG =
      LoggerFactory.getLogger(HCFSFuse.class);

   /**
   * Running this class will mount the file system according to
   * the options passed to this function {@link #parseOptions(String[])}.
   * The user-space fuse application will stay on the foreground and keep
   * the file system mounted. The user can unmount the file system by
   * gracefully killing (SIGINT) the process.
   *
   * @param args arguments to run the command line
   */
  public static void main(String[] args) throws IOException {
    FuseOptions opts = parseOptions(args);
    if (opts == null) {
      System.exit(1);
    }
    Configuration conf = new Configuration();
    for (String confPath : opts.getConfPaths()) {
      conf.addResource(new Path(confPath));
    }

    final FileSystem tfs = new Path(opts.getRoot()).getFileSystem(conf);
    final List<String> fuseOpts = opts.getFuseOpts();
    if (opts.isJniFuseEnable()) {
      final HCFSJniFuseFileSystem fuseFs = new HCFSJniFuseFileSystem(tfs, opts, conf);
      try {
        LOG.info("Mounting HCFSJniFuseFileSystem: mount point=\"{}\", OPTIONS=\"{}\"",
            opts.getMountPoint(), fuseOpts.toArray(new String[0]));
        fuseFs.mount(true, opts.isDebug(), fuseOpts.toArray(new String[0]));
      } catch (FuseException e) {
        LOG.error("Failed to mount {}", opts.getMountPoint(), e);
        // only try to umount file system when exception occurred.
        // jni-fuse registers JVM shutdown hook to ensure fs.umount()
        // will be executed when this process is exiting.
        fuseFs.umount();
      }
    } else {
      fuseOpts.add("-odirect_io");
      LOG.info("mounting to {}", opts.getMountPoint());
<<<<<<< HEAD:src/main/java/net.mbl.hcfsfuse/HCFSFuse.java
      fs.mount(Paths.get(opts.getMountPoint()), true, opts.isDebug(),
              fuseOpts.toArray(new String[0]));
    } catch (FuseException e) {
      LOG.error("Failed to mount {}", opts.getMountPoint(), e);
      // only try to umount file system when exception occurred.
      // jnr-fuse registers JVM shutdown hook to ensure fs.umount()
      // will be executed when this process is exiting.
      fs.umount();
    } finally {
      tfs.close();
=======
      HCFSFuseFileSystem fs = new HCFSFuseFileSystem(tfs, opts, conf);
      try {
        fs.mount(Paths.get(opts.getMountPoint()), true, opts.isDebug(),
            fuseOpts.toArray(new String[0]));
      } catch (FuseException e) {
        LOG.error("Failed to mount {}", opts.getMountPoint(), e);
        // only try to umount file system when exception occurred.
        // jnr-fuse registers JVM shutdown hook to ensure fs.umount()
        // will be executed when this process is exiting.
        fs.umount();
      } finally {
        tfs.close();
      }
>>>>>>> upstream/master:src/main/java/hcfsfuse/fuse/HCFSFuse.java
    }
  }

  private static FuseOptions parseOptions(String[] args) {
    final Options opts = new Options();
    final Option configOpt =
        Option.builder("c")
            .longOpt("config")
            .required(false)
            .hasArg(true)
            .desc("config files.")
            .build();
    final Option mntPoint = Option.builder("m")
        .hasArg()
        .required(true)
        .longOpt("mount-point")
        .desc("Desired local mount point for fuse.")
        .build();

    final Option root = Option.builder("r")
        .hasArg()
        .required(true)
        .longOpt("target-root")
        .desc("Path within target root that will be used as the root of the FUSE mount "
            + "(e.g., /users/foo; defaults to /)")
        .build();

    final Option help = Option.builder("h")
        .required(false)
        .desc("Print this help")
        .build();

    final Option fuseOption = Option.builder("o")
        .valueSeparator(',')
        .required(false)
        .hasArgs()
        .desc("FUSE mount options")
        .build();

    final Option debugOption = Option.builder("debug")
        .required(false)
        .desc("debug flag")
        .build();

    final Option jniFuseOption = Option.builder("jniFuse")
        .required(false)
        .desc("use jnifuse flag")
        .build();

    opts.addOption(configOpt);
    opts.addOption(mntPoint);
    opts.addOption(root);
    opts.addOption(help);
    opts.addOption(fuseOption);
    opts.addOption(debugOption);
    opts.addOption(jniFuseOption);

    final CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cli = parser.parse(opts, args);

      if (cli.hasOption("h")) {
        final HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp(HCFSFuse.class.getName(), opts);
        return null;
      }

      String[] configFiles;
      if (cli.hasOption("config")) {
        configFiles = cli.getOptionValues(configOpt.getOpt());
      } else {
        configFiles = new String[0];
      }

      String mntPointValue = cli.getOptionValue("m");
      String rootValue = cli.getOptionValue("r");

      List<String> fuseOpts = new ArrayList<>();
      if (cli.hasOption("o")) {
        String[] fopts = cli.getOptionValues("o");
        // keep the -o
        for (final String fopt : fopts) {
          fuseOpts.add("-o" + fopt);
        }
      }

      boolean fuseDebug = false;
      if (cli.hasOption("debug")) {
        fuseDebug = true;
      }
      boolean jniFuseEnable = false;
      if (cli.hasOption("jniFuse")) {
        jniFuseEnable = true;
      }
      return new FuseOptions(mntPointValue, rootValue, fuseDebug,
          fuseOpts, configFiles, jniFuseEnable);
    } catch (ParseException e) {
      System.err.println("Error while parsing CLI: " + e.getMessage());
      final HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp(HCFSFuse.class.getName(), opts);
      return null;
    } catch (Exception e) {
      System.err.println("Error while parsing CLI: " + e.getMessage());
      final HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp(HCFSFuse.class.getName(), opts);
      return null;
    }
  }
}
