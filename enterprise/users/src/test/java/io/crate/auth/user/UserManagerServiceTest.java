/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.auth.user;

import io.crate.metadata.UserDefinitions;
import io.crate.metadata.UsersMetaData;
import io.crate.metadata.UsersPrivilegesMetaData;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.junit.Test;

import java.util.Set;

import static io.crate.auth.user.User.CRATE_USER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class UserManagerServiceTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testNullAndEmptyMetaData() {
        // the users list will always contain a crate user
        Set<User> users = UserManagerService.getUsers(null, null);
        assertThat(users, contains(CRATE_USER));

        users = UserManagerService.getUsers(new UsersMetaData(), new UsersPrivilegesMetaData());
        assertThat(users, contains(CRATE_USER));
    }

    @Test
    public void testNewUser() {
        Set<User> users = UserManagerService.getUsers(new UsersMetaData(UserDefinitions.SINGLE_USER_ONLY), new UsersPrivilegesMetaData());
        assertThat(users, containsInAnyOrder(User.of("Arthur"), CRATE_USER));
    }
}
