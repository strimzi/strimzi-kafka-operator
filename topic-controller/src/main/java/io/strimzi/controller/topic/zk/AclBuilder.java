/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    public AclBuilder addWorld(Permission... permissions) {
        if (world == null) {
            world = new ACL();
        }
        world.setId(new Id("world", "anyone"));
        world.setPerms(Permission.encode(permissions));
        return this;
    }

    public AclBuilder anyAuthenticated(Permission... permissions) {
        if (auth == null) {
            auth = new ACL();
        }
        auth.setId(new Id("auth", null));
        auth.setPerms(Permission.encode(permissions));
        return this;
    }

    public AclBuilder addDigest(String username, String password, Permission... permissions) {
        if (digests == null) {
            digests = new HashMap<>();
        }
        ACL a = digests.get(username);
        if (a == null) {
            a = new ACL();
            digests.put(username, a);
        }
        a.setId(new Id("digest", "username"+password));
        a.setPerms(Permission.encode(permissions));
        return this;
    }

    public AclBuilder addHost(String host, Permission... permissions) {
        if (hosts == null) {
            hosts = new HashMap<>();
        }
        ACL a = hosts.get(host);
        if (a == null) {
            a = new ACL();
            hosts.put(host, a);
        }
        a.setId(new Id("host", host));
        a.setPerms(Permission.encode(permissions));
        return this;
    }

    public AclBuilder ip(String addr, int bits, Permission... permissions) {
        if (ips == null) {
            ips = new HashMap<>();
        }
        String cidr = addr+"/"+bits;
        ACL a = ips.get(cidr);
        if (a == null) {
            a = new ACL();
            ips.put(cidr, a);
        }
        a.setId(new Id("ip", cidr));
        a.setPerms(Permission.encode(permissions));
        return this;
    }

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
