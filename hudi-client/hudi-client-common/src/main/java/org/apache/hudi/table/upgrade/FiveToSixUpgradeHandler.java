package org.apache.hudi.table.upgrade;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;
import java.util.Map;

/**
 * Upgrade handle to assist in upgrading hoodie table from version 5 to 6.
 */
public class FiveToSixUpgradeHandler implements UpgradeHandler {

    @Override
    public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
        HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
        HoodieTableMetaClient metaClient = table.getMetaClient();
        // delete compaction requested file from .aux
        HoodieTimeline compactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline()
                .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);
        compactionTimeline.getInstants().stream().forEach(instant -> {
            String fileName = instant.getFileName();
            try {
                metaClient.getFs().delete(new Path(metaClient.getMetaAuxiliaryPath(), fileName));
            } catch (IOException e) {
                throw new HoodieUpgradeDowngradeException("Exception thrown while upgrading Hoodie Table from version 5 to 6", e);
            }
        });
        return null;
    }
}
