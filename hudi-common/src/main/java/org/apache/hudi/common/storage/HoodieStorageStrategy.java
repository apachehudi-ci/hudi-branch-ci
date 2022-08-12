package org.apache.hudi.common.storage;

import java.io.Serializable;

public interface HoodieStorageStrategy extends Serializable {

  String NON_PARTITIONED_NAME = ".";

  /**
   * Return a fully-qualified storage location for the given partition path and file ID.
   *
   * @param partitionPath partition path for the file
   * @param fileId file ID
   * @return a fully-qualified absolute path string for a data file
   */
  String storageLocation(String partitionPath, String fileId);

  /**
   * Return a fully-qualified storage location for the given file ID.
   *
   * @param fileId file ID
   * @return a fully-qualified absolute path string for a data file
   */
  String storageLocation(String fileId);
}
