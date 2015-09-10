package org.apache.falcon.regression.hive.dr;

import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.oozie.client.OozieClient;

/**
 * Enum to represent location of recipe execution.
 */
enum RecipeExecLocation {
    SourceCluster {
        protected OozieClient getRecipeOC(OozieClient srcOC, OozieClient tgtOC) {
            return srcOC;
        }
        protected ClusterMerlin getRecipeCluster(ClusterMerlin srcCM, ClusterMerlin tgtCM) {
            return srcCM;
        }
        protected Bundle getRecipeBundle(Bundle srcBundle, Bundle tgtBundle) {
            return srcBundle;
        }
    },
    TargetCluster {
        protected OozieClient getRecipeOC(OozieClient srcOC, OozieClient tgtOC) {
            return tgtOC;
        }
        protected ClusterMerlin getRecipeCluster(ClusterMerlin srcCM, ClusterMerlin tgtCM) {
            return tgtCM;
        }
        protected Bundle getRecipeBundle(Bundle srcBundle, Bundle tgtBundle) {
            return tgtBundle;
        }
    };

    /** Get oozie client for the Oozie that is going to run the recipe.
     * @param srcOC the oozie client for the source cluster
     * @param tgtOC the oozie client for the target cluster
     * @return oozie client for the Oozie that is going to run the recipe
     */
    abstract protected OozieClient getRecipeOC(OozieClient srcOC, OozieClient tgtOC);

    abstract protected ClusterMerlin getRecipeCluster(ClusterMerlin srcCM, ClusterMerlin tgtCM);

    abstract protected Bundle getRecipeBundle(Bundle srcBundle, Bundle tgtBundle);

}
