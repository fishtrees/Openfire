package org.jivesoftware.openfire.cluster.mq;

import org.apache.activemq.ActiveMQConnectionFactory;

import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.QName;
import org.dom4j.io.XMPPPacketReader;
import org.jivesoftware.openfire.*;
import org.jivesoftware.openfire.auth.UnauthorizedException;
import org.jivesoftware.openfire.carbons.Received;
import org.jivesoftware.openfire.container.BasicModule;
import org.jivesoftware.openfire.forward.Forwarded;
import org.jivesoftware.openfire.session.ClientSession;
import org.jivesoftware.openfire.session.LocalClientSession;
import org.jivesoftware.openfire.session.LocalOutgoingServerSession;
import org.jivesoftware.openfire.session.OutgoingServerSession;
import org.jivesoftware.openfire.spi.ClientRoute;
import org.jivesoftware.util.ConcurrentHashSet;
import org.jivesoftware.util.JiveGlobals;
import org.jivesoftware.util.cache.Cache;
import org.jivesoftware.util.cache.CacheFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;
import org.xmpp.packet.*;
import org.xmpp.packet.Message;

import javax.jms.*;
import javax.jms.Connection;
import java.io.IOException;
import java.io.StringReader;
import java.lang.IllegalStateException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

/**
 * MQ based client session routing table
 * Created by yulin on 2015/3/19.
 */
public class ClientRoutingTable extends BasicModule implements RoutingTable {

    private static final Logger logger = LoggerFactory.getLogger(ClientRoutingTable.class);

    public static final String C2S_CACHE_NAME = "Routing Users Cache";
    public static final String C2S_SESSION_NAME = "Routing User Sessions";
    public static final String MQ_TOPIC_INBOUND = "xmpp/in";

    private static final ThreadLocal<XMPPPacketReader> PARSER_CACHE = new ThreadLocal<XMPPPacketReader>()
    {
        @Override
        protected XMPPPacketReader initialValue()
        {
            final XMPPPacketReader parser = new XMPPPacketReader();
            parser.setXPPFactory( factory );
            return parser;
        }
    };
    /**
     * Reuse the same factory for all the connections.
     */
    private static XmlPullParserFactory factory = null;

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

    private ConcurrentHashMap<String, MessageProducer> messageProducerCache;
    private ConcurrentHashMap<String, MessageConsumer> messageConsumerCache;

    private Session mqSession;
    private Connection mqConnection;
    private MessageProducer inboundProducer;

    private LocalRoutingTable localRoutingTable;
    private String xmppDomain;
    private XMPPServer server;

    public ClientRoutingTable() {
        super("MQRoutingTable");

        usersCache = CacheFactory.createCache(C2S_CACHE_NAME);
        usersSessions = CacheFactory.createCache(C2S_SESSION_NAME);
        messageProducerCache = new ConcurrentHashMap<String, MessageProducer>();
        messageConsumerCache = new ConcurrentHashMap<String, MessageConsumer>();
        localRoutingTable = new LocalRoutingTable();
    }

    public void addServerRoute(JID route, LocalOutgoingServerSession destination) {
        logger.debug("addServerRoute, " + route.toFullJID() + ", " + destination.toString());
    }

    public void addComponentRoute(JID route, RoutableChannelHandler destination) {
        logger.debug("addComponentRoute, " + route.toFullJID() + ", " + destination.toString());
    }

    public boolean addClientRoute(JID route, LocalClientSession destination) {
        boolean added = false;
        boolean available = destination.getPresence().isAvailable();
        localRoutingTable.addRoute(route.toString(), destination);
        Lock lockU = CacheFactory.getLock(route.toString(), usersCache);
        try {
            lockU.lock();
            added = usersCache.put(route.toString(), new ClientRoute(server.getNodeID(), available)) == null;
        } finally {
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
            } finally {
                lock.unlock();
            }
            createMqPeers(route);
        }

        logger.debug("route: " + route.toString() + " added.");

        return added;
    }

    private void createMqPeers(JID route) {
//        // 创建入站消息发布者
//        String inTopic = getInboundMqTopicName(route);
//        try {
//            // Create the destination (Topic or Queue)
//            Destination mqDest = mqSession.createTopic(inTopic);
//
//            // Create a MessageProducer from the Session to the Topic or Queue
//            MessageProducer producer = mqSession.createProducer(mqDest);
//            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
//
//            messageProducerCache.put(inTopic, producer);
//        } catch (JMSException e) {
//            logger.error("addClientRoute: create mq producer failed, " + e.getMessage(), e);
//        }

        // 订阅出站消息
        String outTopic = getOutboundMqTopicName(route);
        try {
            // Create the destination (Topic or Queue)
            Destination mqDest = mqSession.createTopic(outTopic);

            // Create a MessageProducer from the Session to the Topic or Queue
            MessageConsumer consumer = mqSession.createConsumer(mqDest);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(javax.jms.Message message) {
                    // 路由到客户端！
                    if (message instanceof TextMessage) {
                        TextMessage textMessage = (TextMessage) message;
                        String text = null;
                        try {
                            text = textMessage.getText();
                        } catch (JMSException e) {
                            logger.error("decode javax.jms.TextMessage failed. " + e, e);
                        }
                        if (null != text) {
                            XMPPPacketReader reader = PARSER_CACHE.get();
                            try {
                                Element doc = reader.read(new StringReader(text)).getRootElement();
                                String tag = doc.getName();
                                Packet packet = null;
                                if ("message".equals(tag)) {
                                    packet = new Message(doc, false);
                                } else if ("iq".equals(tag)) {
                                    packet = new IQ(doc, false);
                                } else if ("presence".equals(tag)) {
                                    packet = new Presence(doc, false);
                                } else {
                                    // TODO 现在只需要支持这三种，以后扩展可以支持握手阶段时再开发其他的节
                                    logger.error("unsupported stanza, " + tag);
                                }
                                if (null != packet) {
                                    routeToLocalDomain(packet.getTo(), packet, false);
                                }
                            } catch (DocumentException e) {
                                logger.error("unable parse xmpp stanza, " + text, e);
                            } catch (IOException e) {
                                logger.error("unable parse xmpp stanza, " + text, e);
                            } catch (XmlPullParserException e) {
                                logger.error("unable parse xmpp stanza, " + text, e);
                            }
                        }
                        logger.debug("javax.jms.MessageConsumer, Received: " + text);
                    } else {
                        logger.info("javax.jms.MessageConsumer: non-text message received, must ignore");
                    }
                }
            });

            messageConsumerCache.put(outTopic, consumer);
        } catch (JMSException e) {
            logger.error("addClientRoute: create mq producer failed, " + e.getMessage(), e);
        }
    }

    public void routePacket(JID jid, Packet packet, boolean fromServer) throws PacketException {
        // TODO 在Openfire只负责C2S的事务时需要去掉下面的判断
        // 因为Openfire目前除了作为C2S服务器外还负责进行用户验证，自身会生成一些stanza发送给客户端，所以如果
        // 是非Message类型，就需要判断是不是直接可以发送给客户端，而不用到MQ去中转
        if (packet instanceof IQ) {
            IQ iq = (IQ) packet;
            if (iq.getType() == IQ.Type.result || iq.getType() == IQ.Type.error) {
                boolean routed = routeToLocalDomain(iq.getTo(), iq, fromServer);
                if (routed) {
                    return;
                }
            }
        } else if (packet instanceof Presence) {
            boolean routed = routeToLocalDomain(packet.getTo(), packet, fromServer);
            if (routed) {
                return;
            }
        }
        sendToMq(getInboundMqTopicName(jid), packet.toXML());
    }

    private static String getInboundMqTopicName(JID jid) {
        return "xmpp/in" + jid.toBareJID();
    }
    private static String getOutboundMqTopicName(JID jid) {
        return "xmpp/out/" + jid.toBareJID();
    }

    private void sendToMq(String topic, String data) {
        try {
//            MessageProducer producer = getMessageProducer(topic);
            MessageProducer producer = inboundProducer;
            // Create a messages
            TextMessage message = mqSession.createTextMessage(data);
            // Tell the producer to send the message
            producer.send(message);
            logger.info("Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName());
        }
        catch (Exception e) {
            logger.error("sendToMq failed: " + e, e);
        }
    }

    private MessageProducer getMessageProducer(String topic) {
        return messageProducerCache.get(topic);
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
        return (ClientSession) localRoutingTable.getRoute(jid.toString());
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
        List<JID> jids = new ArrayList<JID>();
        if (xmppDomain.equals(route.getDomain())) {
            // Address belongs to local user
            if (route.getResource() != null) {
                // Address is a full JID of a user
                ClientRoute clientRoute = usersCache.get(route.toString());
                if (clientRoute != null) {
                    jids.add(route);
                }
            }
            else {
                // Address is a bare JID so return all AVAILABLE resources of user
                Lock lock = CacheFactory.getLock(route.toBareJID(), usersSessions);
                try {
                    lock.lock(); // temporarily block new sessions for this JID
                    Collection<String> sessions = usersSessions.get(route.toBareJID());
                    if (sessions != null) {
                        // Select only available sessions
                        for (String jid : sessions) {
                            ClientRoute clientRoute = usersCache.get(jid);
                            if (clientRoute != null) {
                                jids.add(new JID(jid));
                            }
                        }
                    }
                }
                finally {
                    lock.unlock();
                }
            }
        }
        else {
            // Packet sent to remote server
            logger.warn("getRoutes: Packet sent to remote server, route = " + route + ", requester = " + requester);
            jids.add(route);
        }
        return jids;
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

        String inTopic = getInboundMqTopicName(route);
        String outTopic = getOutboundMqTopicName(route);
        messageProducerCache.remove(inTopic);
        messageConsumerCache.remove(outTopic);

        localRoutingTable.removeRoute(address);

        logger.debug("route: " + route.toString() + " removed.");

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

        logger.info("initialized");
    }

    @Override
    public void start() throws IllegalStateException {
        super.start();

        startJmsClient();

        logger.info("started");
    }

    private void startJmsClient() throws IllegalStateException {
        try {
            // Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616?trace=false&soTimeout=60000");

            // Create a Connection
            Connection connection = connectionFactory.createConnection();
            connection.start();

            // Create a Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            mqConnection = connection;
            mqSession = session;

            Destination destination = mqSession.createTopic(MQ_TOPIC_INBOUND);
            inboundProducer = mqSession.createProducer(destination);
            inboundProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        } catch (JMSException e) {
            logger.error("start jms client failed, " + e.getMessage(), e);
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private void stopJmsClient() throws JMSException {
        if (null != inboundProducer) {
            inboundProducer.close();
        }
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

        logger.info("stopped");
    }

    @Override
    public void destroy() {
        super.destroy();

        this.server = null;

        logger.info("destroyed");
    }

    /**
     * Deliver the message sent to the bare JID of a local user to the best connected resource. If the
     * target user is not online then messages will be stored offline according to the offline strategy.
     * However, if the user is connected from only one resource then the message will be delivered to
     * that resource. In the case that the user is connected from many resources the logic will be the
     * following:
     * <ol>
     *  <li>Select resources with highest priority</li>
     *  <li>Select resources with highest show value (chat, available, away, xa, dnd)</li>
     *  <li>Select resource with most recent activity</li>
     * </ol>
     *
     * Admins can override the above logic and just send the message to all connected resources
     * with highest priority by setting the system property <tt>route.all-resources</tt> to
     * <tt>true</tt>.
     *
     * @param recipientJID the bare JID of the target local user.
     * @param packet the message to send.
     * @return true if at least one target session was found
     */
    private boolean routeToBareJID(JID recipientJID, Message packet, boolean isPrivate) {
        List<ClientSession> sessions = new ArrayList<ClientSession>();
        // Get existing AVAILABLE sessions of this user or AVAILABLE to the sender of the packet
        List<JID> routes = getRoutes(recipientJID, packet.getFrom());
        if (null != routes && routes.size() > 0) {
            for (JID address : routes) {
                ClientSession session = getClientRoute(address);
                // TODO session.isInitialized() 与presence相关，所以暂时不判断该条件
                if (session != null /*&& session.isInitialized()*/) {
                    sessions.add(session);
                }
            }
        }

        // Get the sessions with non-negative priority for message carbons processing.
        List<ClientSession> nonNegativePrioritySessions = getNonNegativeSessions(sessions, 0);

        if (nonNegativePrioritySessions.isEmpty()) {
            // No session is available so store offline
            logger.debug("Unable to route packet. No session is available so store offline. {} ", packet.toXML());
            return false;
        }

        // Check for message carbons enabled sessions and send the message to them.
        for (ClientSession session : nonNegativePrioritySessions) {
            // Deliver to each session, if is message carbons enabled.
            if (shouldCarbonCopyToResource(session, packet, isPrivate)) {
                session.process(packet);
                // Deliver to each session if property route.really-all-resources is true
                // (in case client does not support carbons)
            } else if (JiveGlobals.getBooleanProperty("route.really-all-resources", false)) {
                session.process(packet);
            }
        }

        // Get the highest priority sessions for normal processing.
        List<ClientSession> highestPrioritySessions = getHighestPrioritySessions(nonNegativePrioritySessions);

        if (highestPrioritySessions.size() == 1) {
            // Found only one session so deliver message (if it hasn't already been processed because it has message carbons enabled)
            if (!shouldCarbonCopyToResource(highestPrioritySessions.get(0), packet, isPrivate)) {
                highestPrioritySessions.get(0).process(packet);
            }
        }
        else {
            // Many sessions have the highest priority (be smart now) :)
            if (!JiveGlobals.getBooleanProperty("route.all-resources", false)) {
                // Sort sessions by show value (e.g. away, xa)
                Collections.sort(highestPrioritySessions, new Comparator<ClientSession>() {

                    public int compare(ClientSession o1, ClientSession o2) {
                        int thisVal = getShowValue(o1);
                        int anotherVal = getShowValue(o2);
                        return (thisVal<anotherVal ? -1 : (thisVal==anotherVal ? 0 : 1));
                    }

                    /**
                     * Priorities are: chat, available, away, xa, dnd.
                     */
                    private int getShowValue(ClientSession session) {
                        Presence.Show show = session.getPresence().getShow();
                        if (show == Presence.Show.chat) {
                            return 1;
                        }
                        else if (show == null) {
                            return 2;
                        }
                        else if (show == Presence.Show.away) {
                            return 3;
                        }
                        else if (show == Presence.Show.xa) {
                            return 4;
                        }
                        else {
                            return 5;
                        }
                    }
                });

                // Get same sessions with same max show value
                List<ClientSession> targets = new ArrayList<ClientSession>();
                Presence.Show showFilter = highestPrioritySessions.get(0).getPresence().getShow();
                for (ClientSession session : highestPrioritySessions) {
                    if (session.getPresence().getShow() == showFilter) {
                        targets.add(session);
                    }
                    else {
                        break;
                    }
                }

                // Get session with most recent activity (and highest show value)
                Collections.sort(targets, new Comparator<ClientSession>() {
                    public int compare(ClientSession o1, ClientSession o2) {
                        return o2.getLastActiveDate().compareTo(o1.getLastActiveDate());
                    }
                });

                // Make sure, we don't send the packet again, if it has already been sent by message carbons.
                ClientSession session = targets.get(0);
                if (!shouldCarbonCopyToResource(session, packet, isPrivate)) {
                    // Deliver stanza to session with highest priority, highest show value and most recent activity
                    session.process(packet);
                }
            }
            else {
                for (ClientSession session : highestPrioritySessions) {
                    // Make sure, we don't send the packet again, if it has already been sent by message carbons.
                    if (!shouldCarbonCopyToResource(session, packet, isPrivate)) {
                        session.process(packet);
                    }
                }
            }
        }
        return true;
    }

    /**
     * Returns the sessions that had the highest presence priority that is non-negative.
     *
     * @param sessions the list of user sessions that filter and get the ones with highest priority.
     * @return the sessions that had the highest presence non-negative priority or empty collection
     *         if all were negative.
     */
    private List<ClientSession> getHighestPrioritySessions(List<ClientSession> sessions) {
        int highest = Integer.MIN_VALUE;
        // Get the highest priority amongst the sessions
        for (ClientSession session : sessions) {
            int priority = session.getPresence().getPriority();
            if (priority >= 0 && priority > highest) {
                highest = priority;
            }
        }
        // Get sessions that have the highest priority
        return getNonNegativeSessions(sessions, highest);
    }

    private boolean shouldCarbonCopyToResource(ClientSession session, Message message, boolean isPrivate) {
        return !isPrivate && session.isMessageCarbonsEnabled() && message.getType() == Message.Type.chat;
    }

    /**
     * Gets the non-negative session from a minimal priority.
     *
     * @param sessions The sessions.
     * @param min      The minimal priority.
     * @return The filtered sessions.
     */
    private List<ClientSession> getNonNegativeSessions(List<ClientSession> sessions, int min) {
        if (min < 0) {
            return Collections.emptyList();
        }
        // Get sessions with priority >= min
        List<ClientSession> answer = new ArrayList<ClientSession>(sessions.size());
        for (ClientSession session : sessions) {
            if (session.getPresence().getPriority() >= min) {
                answer.add(session);
            }
        }
        return answer;
    }


    /**
     * Routes packets that are sent to the XMPP domain itself (excluding subdomains).
     *
     * @param jid
     *            the recipient of the packet to route.
     * @param packet
     *            the packet to route.
     * @param fromServer
     *            true if the packet was created by the server. This packets
     *            should always be delivered
     * @throws PacketException
     *             thrown if the packet is malformed (results in the sender's
     *             session being shutdown).
     * @return <tt>true</tt> if the packet was routed successfully,
     *         <tt>false</tt> otherwise.
     */
    private boolean routeToLocalDomain(JID jid, Packet packet,
                                       boolean fromServer) {
        boolean routed = false;
        Element privateElement = packet.getElement().element(QName.get("private", "urn:xmpp:carbons:2"));
        boolean isPrivate = privateElement != null;
        // The receiving server and SHOULD remove the <private/> element before delivering to the recipient.
        packet.getElement().remove(privateElement);

        if (jid.getResource() == null) {
            // Packet sent to a bare JID of a user
            if (packet instanceof Message) {
                // Find best route of local user
                routed = routeToBareJID(jid, (Message) packet, isPrivate);
            }
            else {
                throw new PacketException("Cannot route packet of type IQ or Presence to bare JID: " + packet.toXML());
            }
        }
        else {
            // Packet sent to local user (full JID)
            ClientRoute clientRoute = usersCache.get(jid.toString());
            if (clientRoute != null) {
                if (localRoutingTable.isLocalRoute(jid)) {
                    if (packet instanceof Message) {
                        Message message = (Message) packet;
                        if (message.getType() == Message.Type.chat && !isPrivate) {
                            List<JID> routes = getRoutes(jid.asBareJID(), null);
                            for (JID route : routes) {
                                // The receiving server MUST NOT send a forwarded copy to the full JID the original <message/> stanza was addressed to, as that recipient receives the original <message/> stanza.
                                if (!route.equals(jid)) {
                                    ClientSession clientSession = getClientRoute(route);
                                    if (clientSession.isMessageCarbonsEnabled()) {
                                        Message carbon = new Message();
                                        // The wrapping message SHOULD maintain the same 'type' attribute value;
                                        carbon.setType(message.getType());
                                        // the 'from' attribute MUST be the Carbons-enabled user's bare JID
                                        carbon.setFrom(route.asBareJID());
                                        // and the 'to' attribute MUST be the full JID of the resource receiving the copy
                                        carbon.setTo(route);
                                        // The content of the wrapping message MUST contain a <received/> element qualified by the namespace "urn:xmpp:carbons:2", which itself contains a <forwarded/> element qualified by the namespace "urn:xmpp:forward:0" that contains the original <message/>.
                                        carbon.addExtension(new Received(new Forwarded(message)));

                                        try {
                                            localRoutingTable.getRoute(route.toString()).process(carbon);
                                        } catch (UnauthorizedException e) {
                                            logger.error("Unable to route packet " + packet.toXML(), e);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // This is a route to a local user hosted in this node
                    try {
                        localRoutingTable.getRoute(jid.toString()).process(packet);
                        routed = true;
                    } catch (UnauthorizedException e) {
                        logger.error("Unable to route packet " + packet.toXML(), e);
                    }
                }
            }
        }
        return routed;
    }
}
