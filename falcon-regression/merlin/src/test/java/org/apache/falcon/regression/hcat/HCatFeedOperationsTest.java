/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.regression.hcat;

import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.FreqType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HCatUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.falcon.regression.core.util.AssertUtil.assertFailed;
import static org.apache.falcon.regression.core.util.AssertUtil.assertPartial;
import static org.apache.falcon.regression.core.util.AssertUtil.assertSucceeded;
import static org.apache.falcon.regression.core.util.AssertUtil.checkStatus;

/**
 * Tests with operations with hcat feed.
 */
@Test(groups = "embedded")
public class HCatFeedOperationsTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private HCatClient clusterHC;

    private ColoHelper cluster2 = servers.get(1);
    private OozieClient cluster2OC = serverOC.get(1);
    private HCatClient cluster2HC;

    private String dbName = "default";
    private String tableName = "hcatFeedOperationsTest";
    private String randomTblName = "randomTable_HcatFeedOperationsTest";
    private FeedMerlin feed;
    private String aggregateWorkflowDir = cleanAndGetTestDir() + "/aggregator";

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        clusterHC = cluster.getClusterHelper().getHCatClient();
        cluster2HC = cluster2.getClusterHelper().getHCatClient();
        //create an empty table for feed operations
        ArrayList<HCatFieldSchema> partitions = new ArrayList<>();
        partitions.add(HCatUtil.getStringSchema("year", "yearPartition"));
        createEmptyTable(clusterHC, dbName, tableName, partitions);

        //A random table to test submission of replication feed when table doesn't exist on target
        createEmptyTable(clusterHC, dbName, randomTblName, partitions);

        //create empty table on target cluster
        createEmptyTable(cluster2HC, dbName, tableName, new ArrayList<HCatFieldSchema>());
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        Bundle bundle = BundleUtil.readHCatBundle();
        bundles[0] = new Bundle(bundle, cluster.getPrefix());
        bundles[0].generateUniqueBundle(this);
        bundles[0].setClusterInterface(Interfacetype.REGISTRY, cluster.getClusterHelper().getHCatEndpoint());

        bundles[1] = new Bundle(bundle, cluster2.getPrefix());
        bundles[1].generateUniqueBundle(this);
        bundles[1].setClusterInterface(Interfacetype.REGISTRY, cluster2.getClusterHelper().getHCatEndpoint());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws HCatException {
        removeTestClassEntities();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass() throws IOException {
        clusterHC.dropTable(dbName, tableName, true);
        clusterHC.dropTable(dbName, randomTblName, true);
        cluster2HC.dropTable(dbName, tableName, true);
    }

    /**
     * Submit Hcat feed when Hcat table mentioned in table uri does not exist. Response should reflect failure.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void submitFeedWhenTableDoesNotExist() throws Exception {
        Bundle.submitCluster(bundles[1]);
        feed = bundles[1].getInputFeedFromBundle();
        feed.setTableValue(dbName, randomTblName, FreqType.YEARLY.getHcatPathValue());
        ServiceResponse response = prism.getFeedHelper().submitEntity(feed);
        assertFailed(response);
    }

    /**
     * Submit Hcat feed when Hcat table mentioned in table uri exists. Delete that feed, and re-submit.
     * All responses should reflect success.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void submitFeedPostDeletionWhenTableExists() throws Exception {
        Bundle.submitCluster(bundles[0]);
        feed = bundles[0].getInputFeedFromBundle();
        feed.setTableValue(dbName, tableName, FreqType.YEARLY.getHcatPathValue());
        ServiceResponse response = prism.getFeedHelper().submitEntity(feed);
        assertSucceeded(response);

        response = prism.getFeedHelper().delete(feed.getName());
        assertSucceeded(response);

        response = prism.getFeedHelper().submitEntity(feed);
        assertSucceeded(response);
    }

    /**
     * Submit Hcat Replication feed when Hcat table mentioned in table uri does not exist on target. The response is
     * Partial, with successful with submit/schedule on source.
     *
     * @throws Exception
     */
    @Test
    public void submitAndScheduleReplicationFeedWhenTableDoesNotExistOnTarget() throws Exception {
        Bundle.submitCluster(bundles[0], bundles[1]);
        final String startDate = "2010-01-01T20:00Z";
        final String endDate = "2099-01-01T00:00Z";
        String tableUri = "catalog:" + dbName + ":" + randomTblName + "#year=${YEAR}";
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, endDate);
        bundles[0].setInputFeedTableUri(tableUri);

        feed = bundles[0].getFeeds().get(0);
        // set cluster 2 as the target.
        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(bundles[1].getClusters().get(0).getName())
                .withRetention("months(9000)", ActionType.DELETE)
                .withValidity(startDate, endDate)
                .withClusterType(ClusterType.TARGET)
                .withTableUri(tableUri)
                .build());

        assertPartial(prism.getFeedHelper().submitAndSchedule(feed));
    }

    /**
     * Submit Hcat Replication feed when Hcat table mentioned in table uri exists on both source and target.
     * The response is  Psucceeded, and a replication co-rdinator should apear on target oozie.
     * The test however does not ensure that
     * replication goes through.
     *
     * @throws Exception
     */
    @Test
    public void submitAndScheduleReplicationFeedWhenTableExistsOnSourceAndTarget() throws Exception {
        Bundle.submitCluster(bundles[0], bundles[1]);
        final String startDate = "2010-01-01T20:00Z";
        final String endDate = "2099-01-01T00:00Z";
        String tableUri = "catalog:" + dbName + ":" + tableName + "#year=${YEAR}";
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, endDate);
        bundles[0].setInputFeedTableUri(tableUri);

        feed = bundles[0].getFeeds().get(0);
        // set cluster 2 as the target.
        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(bundles[1].getClusters().get(0).getName())
                .withRetention("months(9000)", ActionType.DELETE)
                .withValidity(startDate, endDate)
                .withClusterType(ClusterType.TARGET)
                .withTableUri(tableUri)
                .build());

        assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed));
        InstanceUtil.waitTillInstancesAreCreated(cluster2OC, feed, 0);
        Assert.assertEquals(OozieUtil.checkIfFeedCoordExist(cluster2OC, feed.getName(), "REPLICATION"), 1);
        //This test doesn't wait for replication to succeed.
    }

    /**
     * Submit Hcat Replication feed. Suspend the feed, and check that feed was suspended on
     * both clusters. Now resume feed, and check that status is running on both clusters.
     * The test however does not ensure that replication goes through.
     *
     * @throws Exception
     */
    @Test
    public void suspendAndResumeReplicationFeed() throws Exception {

        submitAndScheduleReplicationFeedWhenTableExistsOnSourceAndTarget();

        assertSucceeded(prism.getFeedHelper().suspend(feed));

        //check that feed suspended on both clusters
        checkStatus(clusterOC, feed, Job.Status.SUSPENDED);
        checkStatus(cluster2OC, feed, Job.Status.SUSPENDED);

        assertSucceeded(prism.getFeedHelper().resume(feed));

        checkStatus(clusterOC, feed, Job.Status.RUNNING);
        checkStatus(cluster2OC, feed, Job.Status.RUNNING);
    }

    /**
     * Submit Hcat Replication feed. Delete the feed, and check that feed was deleted on
     * both clusters. The test however does not ensure that replication goes through.
     *
     * @throws Exception
     */
    @Test
    public void deleteReplicationFeed() throws Exception {
        submitAndScheduleReplicationFeedWhenTableExistsOnSourceAndTarget();

        assertSucceeded(prism.getFeedHelper().delete(feed));
        checkStatus(clusterOC, feed, Job.Status.KILLED);
        checkStatus(cluster2OC, feed, Job.Status.KILLED);
    }


    private static void createEmptyTable(HCatClient cli, String dbName, String tabName,
                                        List<HCatFieldSchema> partitionCols) throws HCatException{

        ArrayList<HCatFieldSchema> cols = new ArrayList<>();
        cols.add(HCatUtil.getStringSchema("id", "id comment"));
        HCatCreateTableDesc tableDesc = HCatCreateTableDesc
                .create(dbName, tabName, cols)
                .partCols(partitionCols)
                .fileFormat("textfile")
                .ifNotExists(true)
                .isTableExternal(true)
                .build();
        cli.createTable(tableDesc);
    }
}
