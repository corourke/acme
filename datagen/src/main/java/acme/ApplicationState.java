package acme;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class ApplicationState {
  // Thread-safe variables
  private final AtomicBoolean runStatus = new AtomicBoolean(false);
  private final AtomicReference<String> outputDirectory = new AtomicReference<>("./");

  // Prototypes
  private final AtomicReference<String> statusMessage = new AtomicReference<>("");
  private final AtomicReference<Object[]> objects = new AtomicReference<>(new Object[0]);

  // Thread-safe setters
  public void setOutputDirectory(String directory) {
    outputDirectory.set(directory);
  }

  public void setRunStatus(boolean newStatus) {
    runStatus.set(newStatus);
  }

  public void setStatusMessage(String newMessage) {
    statusMessage.set(newMessage);
  }

  public void setObjects(Object[] newObjects) {
    objects.set(newObjects);
  }

  // Thread-safe getters
  public String getOutputDirectory() {
    return outputDirectory.get();
  }

  public boolean getRunStatus() {
    return runStatus.get();
  }

  public String getStatusMessage() {
    return statusMessage.get();
  }

  public Object[] getObjects() {
    return objects.get();
  }

}
