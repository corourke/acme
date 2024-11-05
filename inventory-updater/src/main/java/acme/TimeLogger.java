package acme;

import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Usage:
 * import acme.TimeLogger; // Import the new utility class
 * // method that returns a value
 * Type return = TimeLogger.log("methodName", () -> methodName(params));
 * // method with no return value
 * TimeLogger.log("methodName", () -> methodName(params));
 **/

public class TimeLogger {
  private static final long LONG_EXEC_WARN_TIME = 100; // millis
  private static final Logger logger = Logger.getLogger(TimeLogger.class.getName());

  public static <T> T log(String methodName, Supplier<T> method) {
    long startTime = System.currentTimeMillis();
    T result = method.get();
    long duration = System.currentTimeMillis() - startTime;
    if (duration > LONG_EXEC_WARN_TIME) {
      logger.log(Level.WARNING, String.format("%s took %d ms to execute", methodName, duration));
    }
    return result;
  }

  public static void log(String methodName, Runnable method) {
    long startTime = System.currentTimeMillis();
    method.run();
    long duration = System.currentTimeMillis() - startTime;
    if (duration > LONG_EXEC_WARN_TIME) {
      logger.log(Level.WARNING, String.format("%s took %d ms to execute", methodName, duration));
    }
  }
}
