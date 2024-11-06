package acme;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.logging.Logger;
import java.util.Properties;
import java.util.logging.Level;
import java.util.concurrent.CompletableFuture;

public class S3FileUploader {

  private final S3Client s3;
  private static final Logger logger = Logger.getLogger(S3FileUploader.class.getName());
  private final String bucketName;
  private final String folderPath;

  // Note: will look for the AWS credentials in the ~/.aws/credentials file
  // will use the default profile in ~/.aws/config
  public S3FileUploader(Properties props) {
    Region region = Region.of(props.getProperty("region"));
    this.bucketName = props.getProperty("bucketName");
    this.folderPath = props.getProperty("folderPath");

    // TODO: check properties at program load
    if (region == null || bucketName == null || folderPath == null) {
      throw new IllegalArgumentException(
          "Region, bucket name, and folder path must be specified in config.properties.");
    }

    this.s3 = S3Client.builder()
        .region(region)
        .credentialsProvider(ProfileCredentialsProvider.create())
        .build();
  }

  public CompletableFuture<Boolean> uploadFileAsync(String fileName) {

    return CompletableFuture.supplyAsync(() -> {
      try {
        // Check if the file exists and is not a directory
        if (fileName == null || !java.nio.file.Files.exists(Paths.get(fileName))
            || java.nio.file.Files.isDirectory(Paths.get(fileName))) {
          logger.log(Level.SEVERE, "Invalid file: " + fileName);
          return false; // Return false if the file is invalid
        }
        // Strip any prefixes from the fileName
        String key = folderPath + "/" + fileName.substring(fileName.lastIndexOf('/') + 1);
        PutObjectRequest putOb = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(key)
            .build();

        s3.putObject(putOb, Paths.get(fileName));
        java.nio.file.Files.delete(Paths.get(fileName)); // delete the file that was uploaded
        logger.log(Level.INFO, "Success " + key);
        return true;
      } catch (S3Exception e) {
        logger.log(Level.SEVERE, "S3 error: " + e.awsErrorDetails().errorMessage());
        return false;
      } catch (IOException e) {
        logger.log(Level.SEVERE, fileName + " - " + e);
        return false;
      }
    });
  }

  public void close() {
    s3.close();
  }
}