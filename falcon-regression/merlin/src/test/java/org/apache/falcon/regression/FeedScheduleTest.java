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

package org.apache.falcon.regression;

import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.falcon.regression.core.util.AssertUtil.assertFailed;
import static org.apache.falcon.regression.core.util.AssertUtil.assertSucceeded;
import static org.apache.falcon.regression.core.util.AssertUtil.checkStatus;


/**
 * Feed schedule tests.
 */
@Test(groups = "embedded")
public class FeedScheduleTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private FeedMerlin feed;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        Bundle.submitCluster(bundles[0]);
        feed = bundles[0].getInputFeedFromBundle();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Tries to schedule already scheduled feed. Request should be considered as correct.
     * Feed status shouldn't change.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void scheduleAlreadyScheduledFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitEntity(feed);
        assertSucceeded(response);

        response = prism.getFeedHelper().schedule(feed);
        assertSucceeded(response);
        checkStatus(clusterOC, feed, Job.Status.RUNNING);

        //now try re-scheduling again
        response = prism.getFeedHelper().schedule(feed);
        assertSucceeded(response);
        checkStatus(clusterOC, feed, Job.Status.RUNNING);
    }

    /**
     * Schedule correct feed. Feed should got running.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void scheduleValidFeed() throws Exception {
        //submit feed
        ServiceResponse response = prism.getFeedHelper().submitEntity(feed);
        assertSucceeded(response);

        //now schedule the thing
        response = prism.getFeedHelper().schedule(feed);
        assertSucceeded(response);
        checkStatus(clusterOC, feed, Job.Status.RUNNING);
    }

    /**
     * Tries to schedule already scheduled and suspended feed. Suspended status shouldn't change.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void scheduleSuspendedFeed() throws Exception {
        assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed));

        //now suspend
        assertSucceeded(prism.getFeedHelper().suspend(feed));
        checkStatus(clusterOC, feed, Job.Status.SUSPENDED);
        //now schedule this!
        assertSucceeded(prism.getFeedHelper().schedule(feed));
        checkStatus(clusterOC, feed, Job.Status.SUSPENDED);
    }

    /**
     * Schedules and deletes feed. Tries to schedule it. Request should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void scheduleKilledFeed() throws Exception {
        assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed));

        //now suspend
        assertSucceeded(prism.getFeedHelper().delete(feed));
        checkStatus(clusterOC, feed, Job.Status.KILLED);
        //now schedule this!
        assertFailed(prism.getFeedHelper().schedule(feed));
    }

    /**
     * Tries to schedule feed which wasn't submitted. Request should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void scheduleNonExistentFeed() throws Exception {
        assertFailed(prism.getFeedHelper().schedule(feed));
    }
}
