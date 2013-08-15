package org.opennms.core.grid.zookeeper;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.opennms.core.grid.Member;
import org.opennms.core.grid.MembershipEvent;
import org.opennms.core.grid.MembershipListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKMemberManager implements CuratorWatcher {
    private static final Logger LOG = LoggerFactory.getLogger(ZKMemberManager.class);
    public static final String MEMBER_PATH = "/onms/members";
    private static final String MEMBER_NODE_PREFIX = "m-";

    private final CuratorFramework m_client;
    private final ZKMember m_localMember;
    private final Map<String, MembershipListener> m_listeners = new ConcurrentHashMap<String, MembershipListener>();
    private final Map<String, ZKMember> m_members = new ConcurrentHashMap<String, ZKMember>();

    public ZKMemberManager(CuratorFramework client) {
        m_client = client;
        m_localMember = setupLocalMember();
        syncMembers();
    }

    public Member getLocalMember() {
        return m_localMember;
    }

    public Set<Member> getGridMembers() {
        Set<Member> members = new HashSet<Member>();
        members.addAll(m_members.values());
        return members;
    }

    public String addMembershipListener(MembershipListener listener) {
        String uuid = UUID.randomUUID().toString();
        m_listeners.put(uuid, listener);
        return uuid;
    }

    public void removeMembershipListener(String registrationId) {
        m_listeners.remove(registrationId);
    }

    private ZKMember setupLocalMember() {
        String localMemberPath = null;

        ZKMember localMember;
        try {
            localMember = new ZKMember();
        } catch (Exception e) {
            throw new ZKException(e);
        }

        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(
                                                                              m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    EnsurePath ensurePath = m_client.newNamespaceAwareEnsurePath(MEMBER_PATH);
                    ensurePath.ensure(m_client.getZookeeperClient());

                    localMemberPath = ZKPaths.makePath(MEMBER_PATH,
                                                       MEMBER_NODE_PREFIX);
                    localMemberPath = m_client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(localMemberPath,
                                                                                                          SerializationUtils.objToBytes(localMember));
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            throw new ZKException(e);
        }

        m_members.put(localMemberPath, localMember);
        return localMember;
    }

    private synchronized void syncMembers() {
        if (m_client.getState() != CuratorFrameworkState.STARTED) {
            LOG.info("Curator client is stopped. Stopping member sync.");
            return;
        }

        List<String> memberNodes = null;
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(
                                                                              m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    memberNodes = m_client.getChildren().usingWatcher(this).forPath(MEMBER_PATH);
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            throw new ZKException(e);
        }

        // Create the set of path for all known members
        Set<String> memberPaths = new HashSet<String>();
        for (String memberNode : memberNodes) {
            memberPaths.add(ZKPaths.makePath(MEMBER_PATH, memberNode));
        }

        // If a member at a given path is not already in the map, add it
        for (String memberPath : memberPaths) {
            if (!m_members.containsKey(memberPath)) {
                handleMemberAdded(memberPath);
            }
        }

        // If a path is in the map, but not in the set, remove it
        for (String memberPath : m_members.keySet()) {
            if (!memberPaths.contains(memberPath)) {
                handleMemberRemoved(memberPath);
            }
        }
    }

    private ZKMember getMemberAt(String path) {
        ZKMember member = null;
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(
                                                                              m_client);
            while (retryLoop.shouldContinue()) {
                member = (ZKMember) SerializationUtils.objFromBytes(m_client.getData().forPath(path));
                retryLoop.markComplete();
            }
        } catch (Exception e) {
            throw new ZKException(e);
        }

        return member;
    }

    private synchronized void handleMemberAdded(String path) {
        ZKMember member = getMemberAt(path);

        m_members.put(path, member);

        for (MembershipListener listener : m_listeners.values()) {
            listener.memberAdded(new ZKMembershipEvent(
                                                       MembershipEvent.MEMBER_ADDED,
                                                       member));
        }
    }

    private synchronized void handleMemberRemoved(String path) {
        ZKMember member = m_members.remove(path);
        if (member == null) {
            LOG.error("No member with path {} was found in the map.", path);
            return;
        }

        for (MembershipListener listener : m_listeners.values()) {
            listener.memberRemoved(new ZKMembershipEvent(
                                                         MembershipEvent.MEMBER_REMOVED,
                                                         member));
        }
    }

    @Override
    public void process(WatchedEvent event) throws Exception {
        switch (event.getType()) {
        case NodeCreated:
            handleMemberAdded(event.getPath());
            break;
        case NodeDeleted:
            handleMemberRemoved(event.getPath());
            break;
        default:
            break;
        }

        syncMembers();
    }

    private static class ZKMembershipEvent extends MembershipEvent {
        private final int m_type;
        private final ZKMember m_member;

        public ZKMembershipEvent(int type, ZKMember member) {
            m_type = type;
            m_member = member;
        }

        @Override
        public int getEventType() {
            return m_type;
        }

        @Override
        public ZKMember getMember() {
            return m_member;
        }
    }
}
