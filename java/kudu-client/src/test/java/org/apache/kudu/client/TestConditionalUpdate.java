/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.kudu.client;

import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.lang.Integer;

import static org.junit.Assert.assertEquals;

public class TestConditionalUpdate {
  public static final Logger LOG = LoggerFactory.getLogger(TestConditionalUpdate.class);

  private KuduClient client;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  private void createExampleTable(String tableName,
                                         ColumnSchema.Updating type) throws Exception {
    // Set up a simple schema.
    List<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
            .key(true)
            .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.INT32)
            .nullable(true)
            .updating(type)
            .build());
    Schema schema = new Schema(columns);

    // Set up the partition schema, which distributes rows to different tablets by hash.
    CreateTableOptions cto = new CreateTableOptions();
    List<String> hashKeys = new ArrayList<>(1);
    hashKeys.add("key");
    int numBuckets = 3;
    cto.addHashPartitions(hashKeys, numBuckets);
    cto.setNumReplicas(3);

    // Create the table.
    client.createTable(tableName, schema, cto);
  }

  private void insertRows(String tableName, int numRows) throws Exception {
    KuduTable table = client.openTable(tableName);
    KuduSession session = client.newSession();

    for (int i = 0; i < numRows; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt("key", i);
      row.addInt("value", i % 10000);
      session.apply(insert);
    }

    // Call session.close() to end the session and ensure the rows are
    // flushed and errors are returned.
    session.close();
    if (session.countPendingErrors() != 0) {
      System.out.println("errors inserting rows");
      org.apache.kudu.client.RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
      org.apache.kudu.client.RowError[] errs = roStatus.getRowErrors();
      int numErrs = Math.min(errs.length, 5);
      LOG.warn("there were errors inserting rows to Kudu");
      LOG.warn("the first few errors follow:");
      for (int i = 0; i < numErrs; i++) {
        LOG.error("error num {}", errs[i]);
      }
      if (roStatus.isOverflowed()) {
        LOG.error("error buffer overflowed: some errors were discarded");
      }
      throw new RuntimeException("error inserting rows to Kudu");
    }
  }

  private void updateRows(String tableName, int numRows,
                          boolean overWrite, ColumnSchema.Updating type,
                          Vector<Integer> key) throws Exception {
    KuduTable table = client.openTable(tableName);
    KuduSession session = client.newSession();
    session.setForceOverwrite(overWrite);
    for (int i = 0; i < numRows; i++) {
      Update update = table.newUpdate();
      PartialRow row = update.getRow();
      row.addInt("key", i);
      row.addInt("value", 100);
      switch (type) {
        case KEEP_MAX:
          if (i <= 100) {
            key.add(i);
          }
          break;
        case KEEP_MIN:
          if (i >= 100) {
            key.add(i);
          }
          break;
        default:
            key.add(i);
      }
      session.apply(update);
    }

    // Call session.close() to end the session and ensure the rows are
    // flushed and errors are returned.
    session.close();
    if (session.countPendingErrors() != 0) {
      System.out.println("errors updateRows");
      org.apache.kudu.client.RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
      org.apache.kudu.client.RowError[] errs = roStatus.getRowErrors();
      int numErrs = Math.min(errs.length, 5);
      LOG.warn("there were errors updating rows to Kudu");
      LOG.warn("the first few errors follow:");
      for (int i = 0; i < numErrs; i++) {
        LOG.error("error number {}", errs[i]);
      }
      if (roStatus.isOverflowed()) {
        LOG.error("error buffer overflowed: some errors were discarded");
      }
      throw new RuntimeException("error updating rows to Kudu");
    }
  }

  private int scanTableAndCheckResults(String tableName, int numRows,
                                              long timestamp,
                                              Map<Integer, Integer> keyValue) throws Exception {
    KuduTable table = client.openTable(tableName);
    Schema schema = table.getSchema();

    // Scan with a predicate on the 'key' column, returning the 'value' and "added" columns.
    List<String> projectColumns = new ArrayList<>(2);
    projectColumns.add("key");
    projectColumns.add("value");
    int lowerBound = 0;
    KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(
            schema.getColumn("key"),
            ComparisonOp.GREATER_EQUAL,
            lowerBound);
    int upperBound = numRows;
    KuduPredicate upperPred = KuduPredicate.newComparisonPredicate(
            schema.getColumn("key"),
            ComparisonOp.LESS,
            upperBound);
    KuduScanner scanner;
    if (timestamp > 0) {
      AsyncKuduScanner.ReadMode readMode = AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT;
      scanner = client.newScannerBuilder(table)
              .setProjectedColumnNames(projectColumns)
              .addPredicate(lowerPred)
              .addPredicate(upperPred)
              .readMode(readMode)
              .snapshotTimestampMicros(timestamp)
              .build();
    } else {
      scanner = client.newScannerBuilder(table)
              .setProjectedColumnNames(projectColumns)
              .addPredicate(lowerPred)
              .addPredicate(upperPred)
              .build();
    }

    // Check the correct number of values and null values are returned, and
    // that the default value was set for the new column on each row.
    // Note: scanning a hash-partitioned table will not return results in primary key order.
    int resultCount = 0;
    int updateCount = 0;
    while (scanner.hasMoreRows()) {
      RowResultIterator results = scanner.nextRows();
      while (results.hasNext()) {
        RowResult result = results.next();
        if (result.getInt("value") == 100) {
          updateCount++;
          keyValue.put(result.getInt("key"), 100);
        }
        resultCount++;
      }
    }

    int expectedResultCount = upperBound - lowerBound;
    assertEquals(resultCount, expectedResultCount);

    return updateCount;
  }

  @Rule
  public TestName name = new TestName();


  @Test(timeout = 1000000)
  @TabletServerConfig(flags = {
          "--flush_threshold_mb=256",
          "--flush_threshold_mb=1",
          "--flush_threshold_secs=30"
  })
  public void testForceOverWrite() throws Exception {
    String tableName = name.getMethodName() + System.currentTimeMillis();

    int numRows = 10000;
    Vector<Integer> valueIndex = new Vector();
    createExampleTable(tableName, ColumnSchema.Updating.KEEP_MAX);
    insertRows(tableName, numRows);
    updateRows(tableName, numRows, true, ColumnSchema.Updating.KEEP_MAX, valueIndex);

    //memory row set, redo, delta memory store
    Map<Integer, Integer> keyValue = new HashMap();
    int latestCount = scanTableAndCheckResults(tableName, numRows, 0, keyValue);
    assertEquals(10000, latestCount);
    assertEquals(10000, keyValue.size()); //all key value is 100
    for (Integer key: valueIndex) {
      assertEquals(0, keyValue.get(key).compareTo(100));
    }

    client.deleteTable(tableName);
  }

  @Test(timeout = 1000000)
  @TabletServerConfig(flags = {
          "--flush_threshold_mb=256",
          "--flush_threshold_mb=1",
          "--flush_threshold_secs=30"
  })
  public void testKeepMax() throws Exception {
    String tableName = name.getMethodName() + System.currentTimeMillis();

    int numRows = 10000;
    Vector<Integer> valueIndex = new Vector(101);
    createExampleTable(tableName, ColumnSchema.Updating.KEEP_MAX);
    insertRows(tableName, numRows);
    updateRows(tableName, numRows, false, ColumnSchema.Updating.KEEP_MAX, valueIndex);

    Map<Integer, Integer> keyValue = new HashMap();
    int latestCount = scanTableAndCheckResults(tableName, numRows, 0, keyValue);
    assertEquals(101, latestCount);
    assertEquals(101, keyValue.size());
    for (Integer key: valueIndex) {
      assertEquals(0, keyValue.get(key).compareTo(100));
    }

    client.deleteTable(tableName);
  }

  @Test(timeout = 1000000)
  @TabletServerConfig(flags = {
          "--flush_threshold_mb=256",
          "--flush_threshold_mb=1",
          "--flush_threshold_secs=30"
  })
  public void testKeepMin() throws Exception {
    String tableName = name.getMethodName() + System.currentTimeMillis();

    int numRows = 10000;
    Vector<Integer> valueIndex = new Vector(9900);
    createExampleTable(tableName, ColumnSchema.Updating.KEEP_MIN);
    insertRows(tableName, numRows);
    updateRows(tableName, numRows, false, ColumnSchema.Updating.KEEP_MIN, valueIndex);

    Map<Integer, Integer> keyValue = new HashMap();
    int latestCount = scanTableAndCheckResults(tableName, numRows, 0, keyValue);
    assertEquals(9900, latestCount);
    assertEquals(9900, keyValue.size());
    for (Integer key: valueIndex) {
      assertEquals(0, keyValue.get(key).compareTo(100));
    }

    client.deleteTable(tableName);
  }

  @Test(timeout = 1000000)
  @TabletServerConfig(flags = {
          "--flush_threshold_mb=256",
          "--flush_threshold_mb=1",
          "--flush_threshold_secs=30"
  })
  public void testOverWrite() throws Exception {
    String tableName = name.getMethodName() + System.currentTimeMillis();

    int numRows = 10000;
    Vector<Integer> valueIndex = new Vector(10000);
    createExampleTable(tableName, ColumnSchema.Updating.OVERWRITE);
    insertRows(tableName, numRows);
    updateRows(tableName, numRows, false, ColumnSchema.Updating.OVERWRITE, valueIndex);

    Map<Integer, Integer> keyValue = new HashMap();
    int latestCount = scanTableAndCheckResults(tableName, numRows, 0, keyValue);
    assertEquals(10000, latestCount);
    assertEquals(10000, keyValue.size());
    for (Integer key: valueIndex) {
      assertEquals(0, keyValue.get(key).compareTo(100));
    }

    client.deleteTable(tableName);
  }
}
