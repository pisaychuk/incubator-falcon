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

package org.apache.falcon.regression.security;

import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.regression.core.helpers.entity.AbstractEntityHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.CleanupUtil;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * All the falcon operation are implemented as enum. The benefit of this is that these operations
 * can now be passed as parameters.
 */
enum EntityOp {
    status() {
        @Override
        public boolean executeAs(String user, AbstractEntityHelper helper, Entity entity) {
            final ServiceResponse response;
            try {
                response = helper.getStatus(entity.getName(), user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught Exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    dependency() {
        @Override
        public boolean executeAs(String user, AbstractEntityHelper helper, Entity entity) {
            final ServiceResponse response;
            try {
                response = helper.getEntityDependencies(entity.getName(), user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    listing() {
        @Override
        public boolean executeAs(String user, AbstractEntityHelper helper, Entity entity) {
            final String entityName = entity.getName();
            final List<String> entities;
            entities = CleanupUtil.getAllEntitiesOfOneType(helper, user);
            if (entities == null) {
                return false;
            }
            logger.info("Checking for presence of " + entityName + " in " + entities);
            return entities.contains(entityName);
        }
    },
    definition() {
        @Override
        public boolean executeAs(String user, AbstractEntityHelper helper, Entity entity) {
            final ServiceResponse response;
            try {
                response = helper.getEntityDefinition(entity.getName(), user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    delete() {
        @Override
        public boolean executeAs(String user, AbstractEntityHelper helper, Entity entity) {
            final ServiceResponse response;
            try {
                response = helper.delete(entity.getName(), user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    update() {
        @Override
        public boolean executeAs(String user, AbstractEntityHelper helper, Entity entity) {
            final ServiceResponse response;
            try {
                response = helper.update(entity.toString(), entity.toString(), user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    schedule() {
        @Override
        public boolean executeAs(String user, AbstractEntityHelper helper, Entity entity) {
            final ServiceResponse response;
            try {
                response = helper.schedule(entity.getName(), user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    submit() {
        @Override
        public boolean executeAs(String user, AbstractEntityHelper helper, Entity entity) {
            final ServiceResponse response;
            try {
                response = helper.submitEntity(entity.toString(), user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    submitAndSchedule() {
        @Override
        public boolean executeAs(String user, AbstractEntityHelper helper, Entity entity) {
            final ServiceResponse response;
            try {
                response = helper.submitAndSchedule(entity, user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    suspend() {
        @Override
        public boolean executeAs(String user, AbstractEntityHelper helper, Entity entity) {
            final ServiceResponse response;
            try {
                response = helper.suspend(entity.getName(), user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    },
    resume() {
        @Override
        public boolean executeAs(String user, AbstractEntityHelper helper, Entity entity) {
            final ServiceResponse response;
            try {
                response = helper.resume(entity.getName(), user);
            } catch (IOException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (URISyntaxException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (AuthenticationException e) {
                logger.error("Caught exception: " + e);
                return false;
            } catch (InterruptedException e) {
                logger.error("Caught exception: " + e);
                return false;
            }
            return AssertUtil.checkSucceeded(response);
        }
    };

    private static Logger logger = Logger.getLogger(EntityOp.class);
    public abstract boolean executeAs(String user, AbstractEntityHelper helper, Entity entity);
}
