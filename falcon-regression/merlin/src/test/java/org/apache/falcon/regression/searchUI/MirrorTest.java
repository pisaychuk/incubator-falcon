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

package org.apache.falcon.regression.searchUI;

import org.apache.falcon.cli.FalconCLI;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.RecipeMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.MirrorWizardPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.util.Arrays;

public class MirrorTest extends BaseUITestClass {
    private static final Logger LOGGER = Logger.getLogger(MirrorTest.class);
    private final String baseTestDir = cleanAndGetTestDir();
    private final String hdfsSrcDir = cleanAndGetTestDir() + "/hdfsSrcDir";
    private final String hdfsTgtDir = cleanAndGetTestDir() + "/hdfsTgtDir";
    private static final String DB_NAME = "MirrorTest";
    private final ColoHelper cluster = servers.get(0);
    private final ColoHelper cluster2 = servers.get(1);
    private final FileSystem clusterFS = serverFS.get(0);
    private final FileSystem clusterFS2 = serverFS.get(1);
    private final OozieClient clusterOC = serverOC.get(0);
    private final OozieClient clusterOC2 = serverOC.get(1);
    private HCatClient clusterHC;
    private HCatClient clusterHC2;
    RecipeMerlin recipeMerlin;
    Connection connection;
    Connection connection2;
    MirrorWizardPage mirrorPage;
    /**
     * Submit one cluster, 2 feeds and 10 processes with 1 to 10 tags (1st process has 1 tag,
     * 2nd - two tags.. 10th has 10 tags).
     * @throws URISyntaxException
     * @throws IOException
     * @throws AuthenticationException
     * @throws InterruptedException
     * @throws JAXBException
     */
    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
/*
        clusterHC = cluster.getClusterHelper().getHCatClient();
        clusterHC2 = cluster2.getClusterHelper().getHCatClient();
*/
        bundles[0] = BundleUtil.readHCatBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[1] = new Bundle(bundles[0], cluster2);
        bundles[0].generateUniqueBundle(this);
        bundles[1].generateUniqueBundle(this);
        final ClusterMerlin srcCluster = bundles[0].getClusterElement();
        final ClusterMerlin tgtCluster = bundles[1].getClusterElement();
        Bundle.submitCluster(bundles[0], bundles[1]);

        recipeMerlin = RecipeMerlin.readFromDir("HiveDrRecipe",
            FalconCLI.RecipeOperation.HIVE_DISASTER_RECOVERY)
            .withRecipeCluster(srcCluster);
        recipeMerlin.withSourceCluster(srcCluster)
            .withTargetCluster(tgtCluster)
            .withFrequency(new Frequency("5", Frequency.TimeUnit.minutes))
            .withValidity(TimeUtil.getTimeWrtSystemTime(-5), TimeUtil.getTimeWrtSystemTime(5));
        recipeMerlin.setUniqueName(this.getClass().getSimpleName());
        recipeMerlin.withSourceDb(DB_NAME);
/*
        connection = cluster.getClusterHelper().getHiveJdbcConnection();
        runSql(connection, "drop database if exists hdr_sdb1 cascade");
        runSql(connection, "create database hdr_sdb1");
        runSql(connection, "use hdr_sdb1");

        connection2 = cluster2.getClusterHelper().getHiveJdbcConnection();
        runSql(connection2, "drop database if exists hdr_sdb1 cascade");
        runSql(connection2, "create database hdr_sdb1");
        runSql(connection2, "use hdr_sdb1");
*/

        openBrowser();
        SearchPage searchPage = LoginPage.open(getDriver()).doDefaultLogin();
        mirrorPage = searchPage.getPageHeader().doCreateMirror();
        mirrorPage.checkPage();
    }


    @AfterClass(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        closeBrowser();
    }

    @Test
    public void testHeader() throws Exception {
        mirrorPage.getPageHeader().checkHeader();
    }

    /**
     * Create DB on source with 1 table.
     * Select Dataset type as Hive.
     * Populate all fields (name, source, target, validity etc.) with correct and existing values.
     * Click next. Create mirror.
     * Using get entity definition API check that entity has been created.
     * @throws Exception
     */
    @Test
    public void testHiveDefaultScenario() throws Exception {
        recipeMerlin.withSourceDb(DB_NAME);
        final ClusterMerlin srcCluster = recipeMerlin.getSrcCluster();
        final ClusterMerlin tgtCluster = recipeMerlin.getTgtCluster();
        recipeMerlin.setTags(Arrays.asList("key1=val1", "key2=val2", "key3=val3"));

        mirrorPage.setName(recipeMerlin.getName());
        mirrorPage.setTags(recipeMerlin.getTags());
        mirrorPage.setMirrorType(recipeMerlin.getRecipeOperation());

        mirrorPage.setSrcName(srcCluster.getName());
        mirrorPage.setReplication(recipeMerlin);
        //mirrorPage.setSrcPath(hdfsSrcDir);
        mirrorPage.setTgtName(tgtCluster.getName());
        //mirrorPage.setTgtPath(hdfsTgtDir);
        mirrorPage.setRunLocation(MirrorWizardPage.RunLocation.RUN_AT_SOURCE);
        mirrorPage.setStartTime(recipeMerlin.getValidityStart());
        mirrorPage.setEndTime(recipeMerlin.getValidityEnd());
        mirrorPage.toggleAdvancedOptions();
        mirrorPage.setFrequency(recipeMerlin.getFrequency());
        mirrorPage.setDistCpMaxMaps(recipeMerlin.getDistCpMaxMaps());
        mirrorPage.setReplicationMaxMaps(recipeMerlin.getReplicationMaxMaps());
        mirrorPage.setMaxEvents(recipeMerlin.getMaxEvents());
        mirrorPage.setMaxBandwidth(recipeMerlin.getMapBandwidth());
        mirrorPage.setSourceInfo(recipeMerlin.getSrcCluster());
        mirrorPage.setTargetInfo(recipeMerlin.getTgtCluster());
        mirrorPage.setRetry(recipeMerlin.getRetry());
        mirrorPage.setAcl(recipeMerlin.getAcl());
        mirrorPage.next();
        mirrorPage.save();
    }

}
