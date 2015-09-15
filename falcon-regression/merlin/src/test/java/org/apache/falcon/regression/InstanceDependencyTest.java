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

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.falcon.resource.SchedulableEntityInstance;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


/**
 * Regression for instance running api.
 */
@Test(groups = "embedded")
public class InstanceDependencyTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output-data" + MINUTE_DATE_PATTERN;
    final String startTimeStr = "2010-01-02T01:00Z";
    final String endTimeStr = "2010-01-02T01:11Z";
    List<String> inputFeedNames, outputFeedNames, processNames;
    List<Integer> inputFeedFrequescies;
    // process1(inputFeed1, outputFeed1)
    // p2(outputFeed1, outputFeed2)
    // p3(outputFeed1, outputFeed3)
    private static final Logger LOGGER = Logger.getLogger(ProcessInstanceRunningTest.class);

    private static final Comparator<SchedulableEntityInstance> dependencyComparator =
        new Comparator<SchedulableEntityInstance>() {
            @Override
            public int compare(SchedulableEntityInstance o1, SchedulableEntityInstance o2) {
                int tagDiff = o1.getTags().compareTo(o2.getTags());
                if (tagDiff != 0) {
                    return tagDiff;
                }
                int clusterDiff = o1.getCluster().compareTo(o2.getCluster());
                if (clusterDiff != 0) {
                    return clusterDiff;
                }
                int typeDiff = o1.getEntityType().compareTo(o2.getEntityType());
                if (typeDiff != 0) {
                    return typeDiff;
                }
                int dateDiff = o1.getInstanceTime().compareTo(o2.getInstanceTime());
                if (dateDiff != 0) {
                    return dateDiff;
                }
                return 0;
            }
        };

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        LOGGER.info("in @BeforeClass");
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessValidity(startTimeStr, endTimeStr);
        bundles[0].setProcessPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setOutputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].submitFeedsScheduleProcess(prism);
        final String oldInputFeedName = bundles[0].getInputFeedNameFromBundle();
        final String oldOutputFeedName = bundles[0].getOutputFeedNameFromBundle();
        final String oldProcessName = bundles[0].getProcessName();
        inputFeedFrequescies = Arrays.asList(20, 5, 5);
        inputFeedNames = Arrays.asList(oldInputFeedName, oldOutputFeedName, oldOutputFeedName + "-2");
        outputFeedNames = Arrays.asList(oldOutputFeedName, oldOutputFeedName + "-2", oldOutputFeedName + "-3");
        processNames = Arrays.asList(oldProcessName, oldProcessName + "-2", oldProcessName + "-3");
        List<String> feedOutputPaths = Arrays.asList(
            feedOutputPath,
            baseTestHDFSDir + "/output-data-2" + MINUTE_DATE_PATTERN,
            baseTestHDFSDir + "/output-data-3" + MINUTE_DATE_PATTERN
        );

        //create second, third process that consumes output of bundle[0]
        for (int bIndex = 1; bIndex < 3; ++bIndex) {
            final FeedMerlin outputFeed = new FeedMerlin(bundles[0].getOutputFeedFromBundle());
            final ProcessMerlin processMerlin = bundles[0].getProcessObject();

            processMerlin.setName(processNames.get(bIndex));

            outputFeed.setDataLocationPath(feedOutputPaths.get(bIndex));
            outputFeed.setName(outputFeedNames.get(bIndex));

            //rename output feeds before renaming input feeds
            processMerlin.renameFeeds(Collections.singletonMap(oldOutputFeedName, outputFeedNames.get(bIndex)));
            processMerlin.renameFeeds(Collections.singletonMap(oldInputFeedName, inputFeedNames.get(bIndex)));
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(outputFeed.toString()));
            AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(processMerlin.toString()));
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    @Test
    public void processInstanceDependencyTest() throws Exception {
        final String clusterName = bundles[0].getClusterNames().get(0);
        final DateTime startTime = TimeUtil.oozieDateToDate(startTimeStr);
        final DateTime startTimeMinus20 = startTime.minusMinutes(20);

        for (int index = 0; index < 3; ++index) {
            InstanceUtil.waitTillInstanceReachState(clusterOC, processNames.get(index), 3,
                CoordinatorAction.Status.WAITING, EntityType.PROCESS, 5);

            List<SchedulableEntityInstance> expectedDependencies = new ArrayList<>();
            final SchedulableEntityInstance outputInstance =
                new SchedulableEntityInstance(outputFeedNames.get(index), clusterName, startTime.toDate(),
                    EntityType.FEED);
            outputInstance.setTags("Output");
            expectedDependencies.add(outputInstance);
            for (DateTime dt = new DateTime(startTime); !dt.isBefore(startTimeMinus20);
                 dt = dt.minusMinutes(inputFeedFrequescies.get(index))) {
                final SchedulableEntityInstance inputInstance =
                    new SchedulableEntityInstance(inputFeedNames.get(index), clusterName, dt.toDate(), EntityType.FEED);
                inputInstance.setTags("Input");
                expectedDependencies.add(inputInstance);
            }
            InstanceDependencyResult r = prism.getProcessHelper().getInstanceDependencies(processNames.get(index),
                "?instanceTime=" + startTimeStr);

            List<SchedulableEntityInstance> actualDependencies = Arrays.asList(r.getDependencies());
            Collections.sort(expectedDependencies, dependencyComparator);
            Collections.sort(actualDependencies, dependencyComparator);
            Assert.assertEquals(actualDependencies, expectedDependencies,
                "Unexpected dependencies for process: " + processNames.get(index));
        }
    }

    @Test
    public void inputFeedInstanceDependencyTest() throws Exception {
        final String clusterName = bundles[0].getClusterNames().get(0);
        final String inputFeedToTest = inputFeedNames.get(1);
        final DateTime startTime = TimeUtil.oozieDateToDate(startTimeStr);
        final DateTime endTime = TimeUtil.oozieDateToDate(endTimeStr);

        InstanceUtil.waitTillInstanceReachState(clusterOC, processNames.get(1), 3,
            CoordinatorAction.Status.WAITING, EntityType.PROCESS, 5);

        List<SchedulableEntityInstance> expectedDependencies = new ArrayList<>();
        final SchedulableEntityInstance outputInstance =
            new SchedulableEntityInstance(processNames.get(0), clusterName, startTime.toDate(), EntityType.PROCESS);
        outputInstance.setTags("Output");
        expectedDependencies.add(outputInstance);
        final int processFrequency = 5;
        for (DateTime dt = new DateTime(startTime); !dt.isAfter(endTime); dt = dt.plusMinutes(processFrequency)) {
            final SchedulableEntityInstance inputInstance =
                new SchedulableEntityInstance(processNames.get(1), clusterName, dt.toDate(), EntityType.PROCESS);
            inputInstance.setTags("Input");
            expectedDependencies.add(inputInstance);
        }
        InstanceDependencyResult r = prism.getFeedHelper().getInstanceDependencies(inputFeedToTest,
            "?instanceTime=" + startTimeStr);

        List<SchedulableEntityInstance> actualDependencies = Arrays.asList(r.getDependencies());
        Collections.sort(expectedDependencies, dependencyComparator);
        Collections.sort(actualDependencies, dependencyComparator);
        Assert.assertEquals(actualDependencies, expectedDependencies,
            "Unexpected dependencies for process: " + inputFeedToTest);
    }

    @Test
    public void outputFeedInstanceDependencyTest() throws Exception {
        final String clusterName = bundles[0].getClusterNames().get(0);
        final String outputFeedToTest = outputFeedNames.get(1);
        final DateTime startTime = TimeUtil.oozieDateToDate(startTimeStr);
        final DateTime endTime = TimeUtil.oozieDateToDate(endTimeStr);

        InstanceUtil.waitTillInstanceReachState(clusterOC, processNames.get(1), 3,
            CoordinatorAction.Status.WAITING, EntityType.PROCESS, 5);

        List<SchedulableEntityInstance> expectedDependencies = new ArrayList<>();
        final SchedulableEntityInstance outputInstance =
            new SchedulableEntityInstance(processNames.get(1), clusterName, startTime.toDate(), EntityType.PROCESS);
        outputInstance.setTags("Output");
        expectedDependencies.add(outputInstance);
        final int processFrequency = 5;
        for (DateTime dt = new DateTime(startTime); !dt.isAfter(endTime); dt = dt.plusMinutes(processFrequency)) {
            final SchedulableEntityInstance inputInstance =
                new SchedulableEntityInstance(processNames.get(2), clusterName, dt.toDate(), EntityType.PROCESS);
            inputInstance.setTags("Input");
            expectedDependencies.add(inputInstance);
        }
        InstanceDependencyResult r = prism.getFeedHelper().getInstanceDependencies(outputFeedToTest,
            "?instanceTime=" + startTimeStr);

        List<SchedulableEntityInstance> actualDependencies = Arrays.asList(r.getDependencies());
        Collections.sort(expectedDependencies, dependencyComparator);
        Collections.sort(actualDependencies, dependencyComparator);
        Assert.assertEquals(actualDependencies, expectedDependencies,
            "Unexpected dependencies for process: " + outputFeedToTest);
    }

}
