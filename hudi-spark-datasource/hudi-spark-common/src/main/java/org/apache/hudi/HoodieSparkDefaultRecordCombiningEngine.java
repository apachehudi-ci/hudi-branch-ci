package org.apache.hudi;

import org.apache.hudi.commmon.model.HoodieSparkRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Properties;

public class HoodieSparkDefaultRecordCombiningEngine extends HoodieSparkRecordCombiningEngine {

  @Override
  public Option<HoodieRecord> combineAndGetUpdateValue(HoodieRecord older, HoodieRecord newer, Schema schema, Properties props) throws IOException {
    assert older instanceof HoodieSparkRecord;
    assert newer instanceof HoodieSparkRecord;

    // Null check is needed here to support schema evolution. The record in storage may be from old schema where
    // the new ordering column might not be present and hence returns null.
    if (!needUpdatingPersistedRecord((HoodieSparkRecord) older, (HoodieSparkRecord) newer, props)) {
      return Option.of(older);
    }

    if (newer.getData().equals(HoodieSparkRecord.EMPTY)) {
      return Option.empty();
    } else {
      return Option.of(newer);
    }
  }

  protected boolean needUpdatingPersistedRecord(HoodieSparkRecord currentRecord,
      HoodieSparkRecord incomingRecord, Properties properties) {
    /*
     * Combining strategy here returns currentRecord on disk if incoming record is older.
     * The incoming record can be either a delete (sent as an upsert with _hoodie_is_deleted set to true)
     * or an insert/update record. In any case, if it is older than the record in disk, the currentRecord
     * in disk is returned (to be rewritten with new commit time).
     *
     * NOTE: Deletes sent via EmptyHoodieRecordPayload and/or Delete operation type do not hit this code path
     * and need to be dealt with separately.
     */
    Object persistedOrderingVal = currentRecord.getOrderingValue();
    Comparable incomingOrderingVal = incomingRecord.getOrderingValue();
    return persistedOrderingVal == null || incomingOrderingVal == null || ((Comparable) persistedOrderingVal).compareTo(incomingOrderingVal) <= 0;
  }
}
