/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hyte.activemq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URI;
import java.util.Enumeration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.DestinationView;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueueBrowsingTest {

    private BrokerService broker;
    private URI connectUri;
    private ActiveMQConnectionFactory factory;
    private final int maxPageSize = 100;

    @BeforeEach
    public void startBroker() throws Exception {
        broker = createBroker();
        TransportConnector connector = broker.addConnector("tcp://0.0.0.0:0");
        broker.deleteAllMessages();
        broker.start();
        broker.waitUntilStarted();

        PolicyEntry policy = new PolicyEntry();
        policy.setMaxPageSize(maxPageSize);
        broker.setDestinationPolicy(new PolicyMap());
        broker.getDestinationPolicy().setDefaultEntry(policy);

        connectUri = connector.getConnectUri();
        factory = new ActiveMQConnectionFactory(connectUri);
        factory.setWatchTopicAdvisories(false);
        factory.getRedeliveryPolicy().setInitialRedeliveryDelay(0l);
        factory.getRedeliveryPolicy().setRedeliveryDelay(0l);
        factory.getRedeliveryPolicy().setMaximumRedeliveryDelay(0l);
    }

    public BrokerService createBroker() throws IOException {
        return new BrokerService();
    }

    @AfterEach
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

   
    @Test // https://issues.apache.org/jira/browse/AMQ-9554
    public void testRedeliveryInconsistent() throws Exception {

        IndividualDeadLetterStrategy individualDeadLetterStrategy = new IndividualDeadLetterStrategy();
        individualDeadLetterStrategy.setQueuePrefix("");
        individualDeadLetterStrategy.setQueueSuffix(".dlq");
        individualDeadLetterStrategy.setUseQueueForQueueMessages(true);
        broker.getDestinationPolicy().getDefaultEntry().setDeadLetterStrategy(individualDeadLetterStrategy);
        broker.getDestinationPolicy().getDefaultEntry().setPersistJMSRedelivered(true);

        String messageId = null;

        String queueName = "browse.redeliverd.tx";
        String dlqQueueName = "browse.redeliverd.tx.dlq";
        String dlqDlqQueueName = "browse.redeliverd.tx.dlq.dlq";

        ActiveMQQueue queue = new ActiveMQQueue(queueName + "?consumer.prefetchSize=0");
        ActiveMQQueue queueDLQ = new ActiveMQQueue(dlqQueueName + "?consumer.prefetchSize=0");

        broker.getAdminView().addQueue(queueName);
        broker.getAdminView().addQueue(dlqQueueName);

        DestinationView dlqQueueView = broker.getAdminView().getBroker().getQueueView(dlqQueueName);
        DestinationView queueView = broker.getAdminView().getBroker().getQueueView(queueName);

        verifyQueueStats(0l, 0l, 0l, dlqQueueView);
        verifyQueueStats(0l, 0l, 0l, queueView);

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(queue);

        Message sendMessage = session.createTextMessage("Hello world!");
        producer.send(sendMessage);
        messageId = sendMessage.getJMSMessageID();
        session.commit();
        producer.close();

        verifyQueueStats(0l, 0l, 0l, dlqQueueView);
        verifyQueueStats(1l, 0l, 1l, queueView);

        // Redeliver message to DLQ
        Message message = null;
        MessageConsumer consumer = session.createConsumer(queue);
        int rollbackCount = 0;
        do {
            message = consumer.receive(2000l);
            if(message != null) {
                session.rollback();
                rollbackCount++;
            }
        } while (message != null);

        assertEquals(Integer.valueOf(7), Integer.valueOf(rollbackCount));
        verifyQueueStats(1l, 0l, 1l, dlqQueueView);
        verifyQueueStats(1l, 1l, 0l, queueView);

        session.commit();
        consumer.close();

        // Increment redelivery counter on the message in the DLQ
        // Close the consumer to force broker to dispatch
        Message messageDLQ = null;
        MessageConsumer consumerDLQ = session.createConsumer(queueDLQ);
        int dlqRollbackCount = 0;
        int dlqRollbackCountLimit = 5;
        do {
            messageDLQ = consumerDLQ.receive(2000l);
            if(messageDLQ != null) {
                session.rollback();
                session.close();
                consumerDLQ.close();
                session = connection.createSession(true, Session.SESSION_TRANSACTED);
                consumerDLQ = session.createConsumer(queueDLQ);
                dlqRollbackCount++;
            }
        } while (messageDLQ != null && dlqRollbackCount < dlqRollbackCountLimit);
        session.commit();
        consumerDLQ.close();

        // Browse in tx mode works when we are at the edge of maxRedeliveries 
        // aka browse does not increment redeliverCounter as expected
        Queue brokerQueueDLQ = resolveQueue(broker, queueDLQ);

        for(int i=0; i<16; i++) {
            QueueBrowser browser = session.createBrowser(queueDLQ);
            Enumeration<?> enumeration = browser.getEnumeration();
            ActiveMQMessage activemqMessage = null;
            int received = 0;
            while (enumeration.hasMoreElements()) {
                activemqMessage = (ActiveMQMessage)enumeration.nextElement();
                received++;
            }
            browser.close();
            assertEquals(Integer.valueOf(1), Integer.valueOf(received));
            assertEquals(Integer.valueOf(6), Integer.valueOf(activemqMessage.getRedeliveryCounter()));

            // Confirm broker-side redeliveryCounter
            QueueMessageReference queueMessageReference = brokerQueueDLQ.getMessage(messageId);
            assertEquals(Integer.valueOf(6), Integer.valueOf(queueMessageReference.getRedeliveryCounter()));
        }

        session.close();
        connection.close();

        // Change redelivery max and the browser will fail
        factory.getRedeliveryPolicy().setMaximumRedeliveries(3);
        final Connection browseConnection = factory.createConnection();
        browseConnection.start();

        final AtomicInteger browseCounter = new AtomicInteger(0);
        final AtomicInteger jmsExceptionCounter = new AtomicInteger(0);

        final Session browseSession = browseConnection.createSession(true, Session.SESSION_TRANSACTED);

        Thread browseThread = new Thread() {
            public void run() {
                
                QueueBrowser browser = null;
                try {
                    browser = browseSession.createBrowser(queueDLQ);
                    Enumeration<?> enumeration = browser.getEnumeration();
                    while (enumeration.hasMoreElements()) {
                        Message message = (Message)enumeration.nextElement();

                        if(Thread.currentThread().isInterrupted()) {
                            Thread.currentThread().interrupt();
                        }

                        if(message != null) {
                            browseCounter.incrementAndGet();
                        }
                    }
                } catch (JMSException e) {
                    jmsExceptionCounter.incrementAndGet();
                } finally {
                    if(browser != null) { try { browser.close(); } catch (JMSException e) { jmsExceptionCounter.incrementAndGet(); } }
                    if(browseSession != null) { try { browseSession.close(); } catch (JMSException e) { jmsExceptionCounter.incrementAndGet(); } }
                    if(browseConnection != null) { try { browseConnection.close(); } catch (JMSException e) { jmsExceptionCounter.incrementAndGet(); } }
                }
            }
        };
        browseThread.start();
        Thread.sleep(2000l);
        browseThread.interrupt();

        assertEquals(Integer.valueOf(0), Integer.valueOf(browseCounter.get()));
        assertEquals(Integer.valueOf(0), Integer.valueOf(jmsExceptionCounter.get()));

        // ActiveMQConsumer sends a poison ack, messages gets moved to .dlq.dlq AND remains on the .dlq
        DestinationView dlqDlqQueueView = broker.getAdminView().getBroker().getQueueView(dlqDlqQueueName);
        verifyQueueStats(1l, 1l, 0l, queueView);
        verifyQueueStats(1l, 0l, 1l, dlqQueueView);
        verifyQueueStats(1l, 0l, 1l, dlqDlqQueueView);
    }

    protected static void verifyQueueStats(long enqueueCount, long dequeueCount, long queueSize, DestinationView queueView) {
        assertEquals(Long.valueOf(enqueueCount), Long.valueOf(queueView.getEnqueueCount()));
        assertEquals(Long.valueOf(dequeueCount), Long.valueOf(queueView.getDequeueCount()));
        assertEquals(Long.valueOf(queueSize), Long.valueOf(queueView.getQueueSize()));
    }

    protected static Queue resolveQueue(BrokerService brokerService, ActiveMQQueue activemqQueue) throws Exception {
        Set<Destination> destinations = brokerService.getBroker().getDestinations(activemqQueue);
        if(destinations == null || destinations.isEmpty()) {
            return null;
        }

        if(destinations.size() > 1) {
            fail("Expected one-and-only one queue for: " + activemqQueue);
        }

        return (Queue)destinations.iterator().next();
    }
}
