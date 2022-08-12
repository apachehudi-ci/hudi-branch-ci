package org.apache.hudi.common.storage;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HoodieObjectStoreStorageStrategy implements HoodieStorageStrategy {

  private static final Logger LOG = LogManager.getLogger(HoodieObjectStoreStorageStrategy.class);

  private static final int HASH_SEED = 0x0e43cd7a;

  private String tableName;
  private String storagePath;

  public HoodieObjectStoreStorageStrategy(HoodieConfig config) {
    this.tableName = config.getString(HoodieTableConfig.HOODIE_TABLE_NAME_KEY);
    this.storagePath = config.getString(HoodieTableConfig.HOODIE_STORAGE_PATH_KEY);
  }

  public String storageLocation(String partitionPath, String fileId) {
    if (StringUtils.isNullOrEmpty(partitionPath) || NON_PARTITIONED_NAME.equals(partitionPath)) {
      // non-partitioned table
      return String.format("%s/%08x/%s", storagePath, hash(fileId), tableName);
    } else {
      String properPartitionPath = FSUtils.stripLeadingSlash(partitionPath);
      return String.format("%s/%08x/%s/%s", storagePath, hash(fileId), tableName, properPartitionPath);
    }
  }

  public String storageLocation(String fileId) {
    return storageLocation(null, fileId);
  }

  protected int hash(String fileId) {
    return HashID.getXXHash32(fileId, HASH_SEED);
  }
}
