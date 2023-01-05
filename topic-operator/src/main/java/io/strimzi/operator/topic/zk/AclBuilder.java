/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.zk;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static java.util.Arrays.asList;

/** Builds ACL permissions */
public class AclBuilder {

    /** Enums for different permissions */
    public enum Permission {

        /** Read permission */
        READ(ZooDefs.Perms.READ),

        /** Write permission */
        WRITE(ZooDefs.Perms.WRITE),

        /** Create permission */
        CREATE(ZooDefs.Perms.CREATE),

        /** Delete permission */
        DELETE(ZooDefs.Perms.DELETE);
        private final int bit;
        Permission(int bit) {
            this.bit = bit;
        }
        static int encode(Permission... permissions) {
            return encode(EnumSet.copyOf(asList(permissions)));
        }
        static int encode(EnumSet<Permission> permissions) {
            int bits = 0;
            for (Permission p : permissions) {
                bits |= p.bit;
            }
            return bits;
        }
    }
    private ACL world;

    protected static final List<ACL> PUBLIC = Collections.singletonList(new ACL(Permission.encode(EnumSet.allOf(Permission.class)), new Id("world", "anyone")));

    /** Constructor */
    public AclBuilder() {

    }

    /**
     * Set the given permissions for all users (including unauthenticated users).
     *
     * @param permissions The permissions.
     * @return This instance.
     */
    public AclBuilder setWorld(Permission... permissions) {
        if (world == null) {
            world = new ACL();
        }
        world.setId(new Id("world", "anyone"));
        world.setPerms(Permission.encode(permissions));
        return this;
    }

    /**
     * Build the result.
     * @return a list of ACLs from the accumulated state.
     */
    public List<ACL> build() {
        List<ACL> result = new ArrayList<>();
        if (world != null) {
            result.add(world);
        }
        return result;
    }
}
