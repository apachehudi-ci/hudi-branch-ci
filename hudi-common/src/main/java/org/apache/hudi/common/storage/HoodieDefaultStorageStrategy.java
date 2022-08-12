package org.apache.hudi.common.storage;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.StringUtils;

public class HoodieDefaultStorageStrategy implements HoodieStorageStrategy {

  private String basePath;

  public HoodieDefaultStorageStrategy(HoodieConfig config) {
    basePath = config.getString("hoodie.base.path");
    assert(!StringUtils.isNullOrEmpty(basePath));
  }

  public String storageLocation(String partitionPath, String fileId) {
    if (StringUtils.isNullOrEmpty(partitionPath)) {
      return basePath;
    }

    String properPartitionPath = FSUtils.stripLeadingSlash(partitionPath);

    return String.format("%s/%s", basePath, properPartitionPath);
  }

  public String storageLocation(String fileId) {
    return storageLocation(null, fileId);
  }
}
