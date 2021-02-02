package hcfsfuse.jnifuse;

/**
 * Exception indicating Fuse errors.
 */
public class FuseException extends RuntimeException {

  /**
   * @param message error message
   */
  public FuseException(String message) {
    super(message);
  }

  /**
   * @param message error message
   * @param cause exception cause
   */
  public FuseException(String message, Throwable cause) {
    super(message, cause);
  }
}

