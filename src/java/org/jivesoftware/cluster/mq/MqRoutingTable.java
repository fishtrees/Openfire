package org.jivesoftware.cluster.mq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.jivesoftware.openfire.*;
import org.jivesoftware.openfire.container.BasicModule;
import org.jivesoftware.openfire.session.ClientSession;
import org.jivesoftware.openfire.session.LocalClientSession;
import org.jivesoftware.openfire.session.LocalOutgoingServerSession;
import org.jivesoftware.openfire.session.OutgoingServerSession;
import org.jivesoftware.openfire.spi.ClientRoute;
import org.jivesoftware.util.ConcurrentHashSet;
import org.jivesoftware.util.cache.Cache;
import org.jivesoftware.util.cache.CacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmpp.packet.JID;
import org.xmpp.packet.Message;
import org.xmpp.packet.Packet;

import javax.jms.*;
import javax.jms.Connection;
import java.lang.IllegalStateException;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * Created by yulin on 2015/3/19.
 */
public class MqRoutingTable extends BasicModule implements RoutingTable {

    private static final Logger logger = LoggerFactory.getLogger(MqRoutingTable.class);

    public static final String C2S_CACHE_NAME = "Routing Users Cache";
    public static final String C2S_SESSION_NAME = "Routing User Sessions";

    /**
     * Cache (unlimited, never expire) that holds list of connected resources of authenticated users
     * (includes anonymous).
     * Key: bare JID, Value: list of full JIDs of the user
     */
    private Cache<String, Collection<String>> usersSessions;

    /**
     * Cache (unlimited, never expire) that holds sessions of user that have authenticated with the server.
     * Key: full JID, Value: {nodeID, available/unavailable}
     */
    private Cache<String, ClientRoute> usersCache;

    private Cache<String, MessageProducer> messageProducerCache;
    private Session mqSession;
    private Connection mqConnection;

    private LocalRoutingTable localRoutingTable;
    private String xmppDomain;
    private XMPPServer server;

    public MqRoutingTable() {
        super("MQRoutingTable");

        usersCache = CacheFactory.createCache(C2S_CACHE_NAME);
        usersSessions = CacheFactory.createCache(C2S_SESSION_NAME);
        localRoutingTable = new LocalRoutingTable();
    }

    public void addServerRoute(JID route, LocalOutgoingServerSession destination) {
        logger.debug("addServerRoute, " + route.toFullJID() + ", " + destination.toString());
    }

    public void addComponentRoute(JID route, RoutableChannelHandler destination) {
        logger.debug("addComponentRoute, " + route.toFullJID() + ", " + destination.toString());
    }

    public boolean addClientRoute(JID route, LocalClientSession destination) {
        boolean added;
        boolean available = destination.getPresence().isAvailable();
        localRoutingTable.addRoute(route.toString(), destination);
        {
            Lock lockU = CacheFactory.getLock(route.toString(), usersCache);
            try {
                lockU.lock();
                added = usersCache.put(route.toString(), new ClientRoute(server.getNodeID(), available)) == null;
            }
            finally {
                lockU.unlock();
            }
            // Add the session to the list of user sessions
            if (route.getResource() != null && (!available || added)) {
                Lock lock = CacheFactory.getLock(route.toBareJID(), usersSessions);
                try {
                    lock.lock();
                    Collection<String> jids = usersSessions.get(route.toBareJID());
                    if (jids == null) {
                        jids = new ConcurrentHashSet<String>();
                    }
                    jids.add(route.toString());
                    usersSessions.put(route.toBareJID(), jids);
                }
                finally {
                    lock.unlock();
                }
            }
        }
        return added;
    }

    public void routePacket(JID jid, Packet packet, boolean fromServer) throws PacketException {
        sendToMq(getMqTopicName(jid), packet.toXML());
    }

    private static String getMqTopicName(JID jid) {
        return "xmpp.packet." + jid.toFullJID();
    }

    private void sendToMq(String topic, String data) {
        try {
            MessageProducer producer = getMessageProducer(topic);
            // Create a messages
            TextMessage message = mqSession.createTextMessage(data);
            // Tell the producer to send the message
            System.out.println("Sent message: "+ message.hashCode() + " : " + Thread.currentThread().getName());
            producer.send(message);
        }
        catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

    private MessageProducer getMessageProducer(String topic) throws JMSException {
        MessageProducer producer = messageProducerCache.get(topic);
        if (null == producer) {
            Lock lock = CacheFactory.getLock(topic, messageProducerCache);
            try {
                lock.lock();
                // Create the destination (Topic or Queue)
                Destination destination = mqSession.createTopic(topic);

                // Create a MessageProducer from the Session to the Topic or Queue
                producer = mqSession.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                messageProducerCache.put(topic, producer);
            } finally {
                lock.unlock();
            }
        }
        return producer;
    }



    public boolean hasClientRoute(JID jid) {
        return usersCache.containsKey(jid.toString()) || isAnonymousRoute(jid);
    }

    public boolean isAnonymousRoute(JID jid) {
        return false;
    }

    public boolean isLocalRoute(JID jid) {
        return localRoutingTable.isLocalRoute(jid);
    }

    public boolean hasServerRoute(JID jid) {
        return false;
    }

    public boolean hasComponentRoute(JID jid) {
        return false;
    }

    public ClientSession getClientRoute(JID jid) {
        // Check if this session is hosted by this cluster node
        ClientSession session = (ClientSession) localRoutingTable.getRoute(jid.toString());
        return session;
    }

    public Collection<ClientSession> getClientsRoutes(boolean onlyLocal) {
        // Add sessions hosted by this cluster node
        Collection<ClientSession> sessions = new ArrayList<ClientSession>(localRoutingTable.getClientRoutes());
        return sessions;
    }

    public OutgoingServerSession getServerRoute(JID jid) {
        return null;
    }

    public Collection<String> getServerHostnames() {
        return null;
    }

    public int getServerSessionsCount() {
        return 0;
    }

    public Collection<String> getComponentsDomains() {
        return null;
    }

    public List<JID> getRoutes(JID route, JID requester) {
        return null;
    }

    public boolean removeClientRoute(JID route) {
        boolean anonymous = false;
        String address = route.toString();
        ClientRoute clientRoute = null;
        Lock lockU = CacheFactory.getLock(address, usersCache);
        try {
            lockU.lock();
            clientRoute = usersCache.remove(address);
        }
        finally {
            lockU.unlock();
        }
        if (clientRoute != null && route.getResource() != null) {
            Lock lock = CacheFactory.getLock(route.toBareJID(), usersSessions);
            try {
                lock.lock();
                if (anonymous) {
                    usersSessions.remove(route.toBareJID());
                }
                else {
                    Collection<String> jids = usersSessions.get(route.toBareJID());
                    if (jids != null) {
                        jids.remove(route.toString());
                        if (!jids.isEmpty()) {
                            usersSessions.put(route.toBareJID(), jids);
                        }
                        else {
                            usersSessions.remove(route.toBareJID());
                        }
                    }
                }
            }
            finally {
                lock.unlock();
            }
        }
        localRoutingTable.removeRoute(address);
        return clientRoute != null;
    }

    public boolean removeServerRoute(JID route) {
        return false;
    }

    public boolean removeComponentRoute(JID route) {
        return false;
    }

    public void setRemotePacketRouter(RemotePacketRouter remotePacketRouter) {

    }

    public RemotePacketRouter getRemotePacketRouter() {
        return null;
    }

    public void broadcastPacket(Message packet, boolean onlyLocal) {
        // Send the message to client sessions connected to this JVM
        for(ClientSession session : localRoutingTable.getClientRoutes()) {
            session.process(packet);
        }
    }

    @Override
    public void initialize(XMPPServer server) {
        super.initialize(server);

        this.server = server;
        this.xmppDomain = server.getServerInfo().getXMPPDomain();
    }

    @Override
    public void start() throws IllegalStateException {
        super.start();

        startJmsClient();
    }

    private void startJmsClient() throws IllegalStateException {
        try {
            // Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616?trace=false&soTimeout=60000");

            // Create a Connection
            Connection connection = null;
            connection = connectionFactory.createConnection();
            connection.start();

            // Create a Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            mqConnection = connection;
            mqSession = session;

        } catch (JMSException e) {
            logger.error("start jms client failed, " + e.getMessage(), e);
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private void stopJmsClient() throws JMSException {
        if (null != mqSession) {
            mqSession.close();
        }
        if (null != mqConnection) {
            mqConnection.close();
        }
    }

    @Override
    public void stop() {
        super.stop();

        try {
            stopJmsClient();
        } catch (JMSException e) {
            logger.error("stop jms client failed, " + e.getMessage(), e);
        }
    }

    @Override
    public void destroy() {
        super.destroy();

        this.server = null;
    }
}
