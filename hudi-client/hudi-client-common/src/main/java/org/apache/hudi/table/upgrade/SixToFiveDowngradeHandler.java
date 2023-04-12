package org.apache.hudi.table.upgrade;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.util.Collections;
import java.util.Map;

/**
 * Downgrade handle to assist in downgrading hoodie table from version 6 to 5.
 */
public class SixToFiveDowngradeHandler implements DowngradeHandler {

    @Override
    public Map<ConfigProperty, String> downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
        HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
        HoodieTableMetaClient metaClient = table.getMetaClient();
        // sync compaction requested file to .aux
        HoodieTimeline compactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline()
                .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);
        compactionTimeline.getInstants().stream().forEach(instant -> {
            String fileName = instant.getFileName();
            FileIOUtils.copy(metaClient.getFs(),
                    new Path(metaClient.getMetaPath(), fileName),
                    new Path(metaClient.getMetaAuxiliaryPath(), fileName));
        });
        return Collections.emptyMap();
    }
}
