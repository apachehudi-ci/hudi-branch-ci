/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client.transaction.lock;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.lock.LockState;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieLockException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

import static org.apache.hudi.common.config.LockConfiguration.DYNAMODB_PARTITION_KEY_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.DYNAMODB_REGION_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.DYNAMODB_TABLE_NAME_PROP_KEY;

/**
 * A DynamoDB based lock. This {@link LockProvider} implementation allows to lock table operations
 * using DynamoDB. Users need to have access to AWS DynamoDB to be able to use this lock.
 */
@NotThreadSafe
public class DynamoDbBasedLockProvider implements LockProvider<LockItem> {

  private static final Logger LOG = LogManager.getLogger(DynamoDbBasedLockProvider.class);

  private final AmazonDynamoDBLockClient client;
  private final String tableName;
  protected LockConfiguration lockConfiguration;
  private volatile LockItem lock;

  public DynamoDbBasedLockProvider(final LockConfiguration lockConfiguration, final Configuration conf) {
    checkRequiredProps(lockConfiguration);
    this.lockConfiguration = lockConfiguration;
    this.tableName = lockConfiguration.getConfig().getString(DYNAMODB_TABLE_NAME_PROP_KEY);
    AmazonDynamoDB dynamoDB = getDynamoClient();
    // build the dynamoDb lock client
    this.client = new AmazonDynamoDBLockClient(
        AmazonDynamoDBLockClientOptions.builder(dynamoDB, tableName)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withLeaseDuration(10000L)
                .withHeartbeatPeriod(3000L)
                .withCreateHeartbeatBackgroundThread(true)
                .build());
    if (!client.lockTableExists()) {
      createLockTableInDynamoDB(dynamoDB, tableName);
    }
  }

  // Only used for testing
  public DynamoDbBasedLockProvider(final LockConfiguration lockConfiguration, final AmazonDynamoDBLockClient client) {
    checkRequiredProps(lockConfiguration);
    this.lockConfiguration = lockConfiguration;
    this.tableName = lockConfiguration.getConfig().getString(DYNAMODB_TABLE_NAME_PROP_KEY);
    this.client = client;
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    LOG.info(generateLogStatement(LockState.ACQUIRING, generateLogSuffixString()));
    try {
      lock = client.acquireLock(AcquireLockOptions.builder(lockConfiguration.getConfig().getString(DYNAMODB_PARTITION_KEY_PROP_KEY))
              .withAdditionalTimeToWaitForLock(time)
              .withTimeUnit(unit)
              .build());
      LOG.info(generateLogStatement(LockState.ACQUIRED, generateLogSuffixString()));
    } catch (HoodieLockException e) {
      throw e;
    } catch (Exception e) {
      throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_ACQUIRE, generateLogSuffixString()), e);
    }
    return lock != null && !lock.isExpired();
  }

  @Override
  public void unlock() {
    try {
      LOG.info(generateLogStatement(LockState.RELEASING, generateLogSuffixString()));
      if (lock == null) {
        return;
      }
      client.releaseLock(lock);
      lock = null;
      LOG.info(generateLogStatement(LockState.RELEASED, generateLogSuffixString()));
    } catch (Exception e) {
      throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_RELEASE, generateLogSuffixString()), e);
    }
  }

  @Override
  public void close() {
    try {
      if (lock != null) {
        client.releaseLock(lock);
        lock = null;
      }
      this.client.close();
    } catch (Exception e) {
      LOG.error(generateLogStatement(LockState.FAILED_TO_RELEASE, generateLogSuffixString()));
    }
  }

  @Override
  public LockItem getLock() {
    return lock;
  }

  private AmazonDynamoDB getDynamoClient() {
    String region = this.lockConfiguration.getConfig().getString(DYNAMODB_REGION_PROP_KEY);
    String endpointURL = "https://dynamodb." + region + ".amazonaws.com";
    AwsClientBuilder.EndpointConfiguration dynamodbEndpoint =
            new AwsClientBuilder.EndpointConfiguration(endpointURL, region);
    return AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(dynamodbEndpoint)
            .withCredentials(new DefaultAWSCredentialsProviderChain())
            .build();
  }

  public static void createLockTableInDynamoDB(AmazonDynamoDB dynamoDB, String tableName) {
    KeySchemaElement partitionKeyElement = new KeySchemaElement();
    partitionKeyElement.setAttributeName("key");
    partitionKeyElement.setKeyType(KeyType.HASH);

    List<KeySchemaElement> keySchema = new ArrayList<>();
    keySchema.add(partitionKeyElement);

    Collection<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(new AttributeDefinition().withAttributeName("key").withAttributeType(ScalarAttributeType.S));

    CreateTableRequest createTableRequest = new CreateTableRequest(tableName, keySchema);
    createTableRequest.setAttributeDefinitions(attributeDefinitions);
    createTableRequest.setBillingMode("PAY_PER_REQUEST");
    dynamoDB.createTable(createTableRequest);

    try {
      TableUtils.waitUntilActive(dynamoDB, tableName);
    } catch (TableUtils.TableNeverTransitionedToStateException | InterruptedException e) {
      throw new HoodieLockException("Created dynamoDB table never transits to active", e);
    }
    LOG.info("Created dynamoDB table " + tableName);
  }

  private void checkRequiredProps(final LockConfiguration config) {
    ValidationUtils.checkArgument(config.getConfig().getString(DYNAMODB_TABLE_NAME_PROP_KEY) != null);
    ValidationUtils.checkArgument(config.getConfig().getString(DYNAMODB_PARTITION_KEY_PROP_KEY) != null);
    ValidationUtils.checkArgument(config.getConfig().getString(DYNAMODB_REGION_PROP_KEY) != null);
  }

  private String generateLogSuffixString() {
    String dynamoDbPartitionKey = this.lockConfiguration.getConfig().getString(DYNAMODB_PARTITION_KEY_PROP_KEY);
    return StringUtils.join("DynamoDb table = ", tableName, ", partition key = ", dynamoDbPartitionKey);
  }

  protected String generateLogStatement(LockState state, String suffix) {
    return StringUtils.join(state.name(), " lock at ", suffix);
  }
}
