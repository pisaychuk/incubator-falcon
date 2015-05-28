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
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.Entities.RecipeMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.supportClasses.NotifyingAssert;
import org.apache.falcon.regression.core.util.AssertUtil;
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
     * Select Dataset type as FileSystem. Select source and target as hdfs.
     * Populate all fields (name, source, target, validity etc.) with correct and existing values.
     * Click next. Create mirror.
     * Using get entity definition API check that entity has been created.
     * @throws Exception
     */
    @Test
    public void testHdfsDefaultScenario() throws Exception {
        final ClusterMerlin srcCluster = bundles[0].getClusterElement();
        final ClusterMerlin tgtCluster = bundles[1].getClusterElement();
        RecipeMerlin hdfsRecipe = RecipeMerlin.readFromDir("HdfsRecipe",
            FalconCLI.RecipeOperation.HDFS_REPLICATION)
            .withRecipeCluster(srcCluster);
        hdfsRecipe.withSourceCluster(srcCluster)
            .withTargetCluster(tgtCluster)
            .withFrequency(new Frequency("5", Frequency.TimeUnit.minutes))
            .withValidity(TimeUtil.getTimeWrtSystemTime(-5), TimeUtil.getTimeWrtSystemTime(5));
        hdfsRecipe.setUniqueName(this.getClass().getSimpleName());
        hdfsRecipe.withSourceDir(hdfsSrcDir).withTargetDir(hdfsTgtDir);
        hdfsRecipe.setTags(Arrays.asList("key1=val1", "key2=val2", "key3=val3"));

        mirrorPage.applyRecipe(hdfsRecipe);
        mirrorPage.next();
        mirrorPage.save();

        AssertUtil.assertSucceeded(prism.getProcessHelper().getStatus(
            createFakeProcessForRecipe(bundles[0].getProcessObject(), recipeMerlin)));
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
        recipeMerlin.setTags(Arrays.asList("key1=val1", "key2=val2", "key3=val3"));
        mirrorPage.applyRecipe(recipeMerlin);
        mirrorPage.next();
        mirrorPage.save();
        AssertUtil.assertSucceeded(prism.getProcessHelper().getStatus(
            createFakeProcessForRecipe(bundles[0].getProcessObject(), recipeMerlin)));
    }

    /**
     * Test recipe with bad acls.
     * Set owner/group as invalid string (utf-8, special chars, number).
     * Check that user is not allowed to go to the next step and has been notified with an alert.
     * Set permissions as 4digit number, negative, string, 000. Check the same.
     */
    @Test
    public void testInvalidAcl() {
        recipeMerlin.setTags(Arrays.asList("key1=val1", "key2=val2", "key3=val3"));
        final String goodAclOwner = MerlinConstants.CURRENT_USER_NAME;
        final String goodAclGroup = MerlinConstants.CURRENT_USER_GROUP;
        final String goodAclPerms = "777";
        mirrorPage.applyRecipe(recipeMerlin);
        NotifyingAssert notifyingAssert = new NotifyingAssert(true);
        for(String badAclOwner: new String[] {"utf8\u20ACchar","speci@l", "123"}) {
            mirrorPage.setAclOwner(badAclOwner);
            notifyingAssert.assertTrue(mirrorPage.isAclOwnerWarningDisplayed(),
                "Expecting invalid owner warning to be displayed for bad acl owner: " + badAclOwner);
            mirrorPage.next(); //should not go through
            if (mirrorPage.getStepNumber() == 2) {
                mirrorPage.silentPrevious();
                mirrorPage.toggleAdvancedOptions();
            }
            mirrorPage.setAclOwner(goodAclOwner);
            notifyingAssert.assertFalse(mirrorPage.isAclOwnerWarningDisplayed(),
                "Expecting invalid owner warning to not be displayed for good acl owner: " + goodAclOwner);
        }

        for(String badAclGroup: new String[] {"utf8\u20ACchar","speci@l", "123"}) {
            mirrorPage.setAclGroup(badAclGroup);
            notifyingAssert.assertTrue(mirrorPage.isAclGroupWarningDisplayed(),
                "Expecting invalid group warning to be displayed for bad acl group: " + badAclGroup);
            mirrorPage.next(); //should not go through
            if (mirrorPage.getStepNumber() == 2) {
                mirrorPage.silentPrevious();
                mirrorPage.toggleAdvancedOptions();
            }
            mirrorPage.setAclGroup(goodAclGroup);
            notifyingAssert.assertFalse(mirrorPage.isAclGroupWarningDisplayed(),
                "Expecting invalid group warning to not be displayed for good acl group: " + goodAclGroup);
        }

        for(String badAclPermission: new String[] {"1234","-123", "str", "000", "1*", "*1"}) {
            mirrorPage.setAclPermission(badAclPermission);
            notifyingAssert.assertTrue(mirrorPage.isAclPermissionWarningDisplayed(),
                "Expecting invalid permission warning to be displayed for bad acl permission: " + badAclPermission);
            mirrorPage.next(); //should not go through
            if (mirrorPage.getStepNumber() == 2) {
                mirrorPage.silentPrevious();
                mirrorPage.toggleAdvancedOptions();
            }
            mirrorPage.setAclPermission(goodAclPerms); //clear error
            notifyingAssert.assertFalse(mirrorPage.isAclPermissionWarningDisplayed(),
                "Expecting invalid permission warning to not be displayed for good acl permission: " + goodAclPerms);
        }
        notifyingAssert.assertAll();
    }

    /**
     * Select Hive as dataset type.
     * Set source/target staging paths as path with invalid pattern, digit, empty value, special/utf-8 symbols. Check
     * that user is not allowed
     to go to the next step and has been notified with an alert.
     */
    @Test
    public void testHiveAdvancedInvalidStaging() {
        recipeMerlin.withSourceDb(DB_NAME);
        recipeMerlin.setTags(Arrays.asList("key1=val1", "key2=val2", "key3=val3"));
        mirrorPage.applyRecipe(recipeMerlin);
        NotifyingAssert notifyingAssert = new NotifyingAssert(true);
        final String goodSrcStaging = recipeMerlin.getSrcCluster().getLocation(ClusterLocationType.STAGING).getPath();
        final String goodTgtStaging = recipeMerlin.getTgtCluster().getLocation(ClusterLocationType.STAGING).getPath();
        final String[] badTestPaths = new String[] {"not_a_path", "", "not/allowed"};
        for (String path : badTestPaths) {
            mirrorPage.setSourceStaging(path);
            //check error
            mirrorPage.next();
            if (mirrorPage.getStepNumber() == 2) {
                notifyingAssert.fail(
                    "Navigation to page 2 should not be allowed as source staging path is bad: " + path);
                mirrorPage.silentPrevious();
                mirrorPage.toggleAdvancedOptions();
            }
            mirrorPage.setSourceStaging(goodSrcStaging);
            //check error disappeared
        }
        for (String path : badTestPaths) {
            mirrorPage.setTargetStaging(path);
            //check error
            mirrorPage.next();
            if (mirrorPage.getStepNumber() == 2) {
                notifyingAssert.fail(
                    "Navigation to page 2 should not be allowed as target staging path is bad: " + path);
                mirrorPage.silentPrevious();
                mirrorPage.toggleAdvancedOptions();
            }
            mirrorPage.setTargetStaging(goodTgtStaging);
            //check error disappeared
        }
        notifyingAssert.assertAll();
    }

    /**
     * Hack to work with process corresponding to recipe
     * @param processMerlin process merlin to be modified
     *                      (ideally we want to get rid of this and use recipe to generate a fake process xml)
     * @param recipe recipe object that need to be faked
     * @return
     */
    private String createFakeProcessForRecipe(ProcessMerlin processMerlin, RecipeMerlin recipe) {
        processMerlin.setName(recipe.getName());
        return processMerlin.toString();
    }


}
