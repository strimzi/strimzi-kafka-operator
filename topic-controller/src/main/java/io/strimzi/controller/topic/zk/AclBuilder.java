/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic.zk;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class AclBuilder {
    public enum Permission {
        READ(ZooDefs.Perms.READ),
        WRITE(ZooDefs.Perms.WRITE),
        CREATE(ZooDefs.Perms.CREATE),
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
    private ACL auth;
    private Map<String, ACL> digests;
    private Map<String, ACL> hosts;
    private Map<String, ACL> ips;

    public static final List<ACL> PUBLIC = Collections.singletonList(new ACL(Permission.encode(EnumSet.allOf(Permission.class)), new Id("world", "anyone")));

    public AclBuilder() {

    }

    public AclBuilder(List<ACL> acls) {
        for (ACL acl: acls) {
            String scheme = acl.getId().getScheme();
            switch (scheme) {
                case "world":
                    world = acl;
                    break;
                case "auth":
                    auth = acl;
                    break;
                case "digest":
                    String[] username = acl.getId().getId().split(":", 2);
                    getDigests().put(username[0], acl);
                    break;
                case "host":
                    getHosts().put(acl.getId().getId(), acl);
                    break;
                case "ip":
                    getIps().put(acl.getId().getId(), acl);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported scheme " + scheme);
            }
        }
    }

    private Map<String, ACL> getDigests() {
        if (digests == null) {
            digests = new HashMap<>();
        }
        return digests;
    }

    /**
     * Set the given permissions for all users (including unauthenticated users).
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
     * Set the given permissions for authenticated users.
     */
    public AclBuilder setAuthenticated(Permission... permissions) {
        if (auth == null) {
            auth = new ACL();
        }
        auth.setId(new Id("auth", null));
        auth.setPerms(Permission.encode(permissions));
        return this;
    }

    /**
     * Set the given permissions for the given user authenticated with the given password.
     */
    public AclBuilder addDigest(String username, String password, Permission... permissions) {
        Map<String, ACL> digests = getDigests();
        ACL a = digests.get(username);
        if (a == null) {
            a = new ACL();
            digests.put(username, a);
        }
        a.setId(new Id("digest", username + ":" + password));
        a.setPerms(Permission.encode(permissions));
        return this;
    }

    /**
     * Set the given permissions for users connecting from the given host
     * (as resolved on the Zookeeper server, from the client's IP address).
     */
    public AclBuilder addHost(String host, Permission... permissions) {
        Map<String, ACL> hosts = getHosts();
        ACL a = hosts.get(host);
        if (a == null) {
            a = new ACL();
            hosts.put(host, a);
        }
        a.setId(new Id("host", host));
        a.setPerms(Permission.encode(permissions));
        return this;
    }

    private Map<String, ACL> getHosts() {
        if (hosts == null) {
            hosts = new HashMap<>();
        }
        return hosts;
    }

    /**
     * Set the given permissions for users connecting from the most
     * significant {@code bits} given IP {@code address}.
     */
    public AclBuilder addIp(String address, int bits, Permission... permissions) {
        Map<String, ACL> ips = getIps();
        String cidr = address + "/" + bits;
        ACL a = ips.get(cidr);
        if (a == null) {
            a = new ACL();
            ips.put(cidr, a);
        }
        a.setId(new Id("ip", cidr));
        a.setPerms(Permission.encode(permissions));
        return this;
    }

    private Map<String, ACL> getIps() {
        if (ips == null) {
            ips = new HashMap<>();
        }
        return ips;
    }

    /**
     * Build a list of ACLs from the accumulated state.
     */
    public List<ACL> build() {
        List<ACL> result = new ArrayList<>();
        if (world != null) {
            result.add(world);
        }
        if (auth != null) {
            result.add(auth);
        }
        for (Map<String, ACL> m : new Map[]{digests, hosts, ips}) {
            if (m != null) {
                result.addAll(m.values());
            }
        }
        return result;
    }
}
