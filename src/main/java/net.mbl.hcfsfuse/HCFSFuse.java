package net.mbl.hcfsfuse;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ru.serce.jnrfuse.FuseException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Main entry point to HCFS-FUSE.
 */
@Slf4j
public class HCFSFuse {

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
    if (StringUtils.isNotBlank(opts.getConfPath())) {
      conf.addResource(new Path(opts.getConfPath()));
    }

    final FileSystem tfs = new Path(opts.getRoot()).getFileSystem(conf);
    HCFSFuseFileSystem fs = new HCFSFuseFileSystem(tfs, opts, conf);
    final List<String> fuseOpts = opts.getFuseOpts();
    try {
      fs.mount(Paths.get(opts.getMountPoint()), true, opts.isDebug(),
          fuseOpts.toArray(new String[0]));
    } catch (FuseException e) {
      log.error("Failed to mount {}", opts.getMountPoint(), e);
      // only try to umount file system when exception occurred.
      // jnr-fuse registers JVM shutdown hook to ensure fs.umount()
      // will be executed when this process is exiting.
      fs.umount();
    } finally {
      tfs.close();
    }
  }

  private static FuseOptions parseOptions(String[] args) {
    final Options opts = new Options();
    final Option configOpt =
        Option.builder("config")
            .longOpt("config")
            .required(false)
            .hasArg(true)
            .desc("config file.")
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

    opts.addOption(configOpt);
    opts.addOption(mntPoint);
    opts.addOption(root);
    opts.addOption(help);
    opts.addOption(fuseOption);
    opts.addOption(debugOption);

    final CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cli = parser.parse(opts, args);

      if (cli.hasOption("h")) {
        final HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp(HCFSFuse.class.getName(), opts);
        return null;
      }

      String configFile = "";
      if (cli.hasOption("config")) {
        configFile = cli.getOptionValue(configOpt.getOpt());
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
      return new FuseOptions(mntPointValue, rootValue, fuseDebug, fuseOpts, configFile);
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
