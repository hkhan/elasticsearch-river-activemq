/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.activemq;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ActivemqRiver extends AbstractRiverComponent implements River {

    /* elasticsearch client */
    private final Client client;

    private final String activemqHost;
    private final int activemqPort;
    private final String activemqUser;
    private final String activemqPassword;

    private final String activemqChannel;

    private volatile boolean closed = false;

    private volatile Thread thread;

    @SuppressWarnings({"unchecked"})
    @Inject
    public ActivemqRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;

        if (settings.settings().containsKey("activemqmq")) {
            Map<String, Object> activemqSettings = (Map<String, Object>) settings.settings().get("activemq");
            activemqHost = XContentMapValues.nodeStringValue(activemqSettings.get("host"), "localhost");
            activemqPort = XContentMapValues.nodeIntegerValue(activemqSettings.get("port"), 61613);
            activemqUser = XContentMapValues.nodeStringValue(activemqSettings.get("user"), "guest");
            activemqPassword = XContentMapValues.nodeStringValue(activemqSettings.get("pass"), "guest");
            activemqChannel = XContentMapValues.nodeStringValue(activemqSettings.get("queue"), "elasticsearch");
        } else {
            activemqHost = "localhost";
            activemqPort = 61613;
            activemqUser = "guest";
            activemqPassword = "guest";
            activemqChannel = "elasticsearch";
        }
    }

    @Override
    public void start() {
        logger.info("creating activemq river, host [{}], port [{}], user [{}]", activemqHost, activemqPort, activemqUser, activemqPassword);

        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "activemqmq_river").newThread(new Consumer());
        thread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing activemqmq river");
        closed = true;
        thread.interrupt();
    }

    private class Consumer implements Runnable {

        private net.ser1.stomp.Client stompClient;

        @Override
        public void run() {
            while (true) {
                if (closed) {
                    return;
                }

                try {
                    stompClient = new net.ser1.stomp.Client(activemqHost, activemqPort, activemqUser, activemqPassword);
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to created a connection / channel", e);
                    } else {
                        continue;
                    }
                    cleanup(0, "failed to connect");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        // ignore, if we are closing, we will exit later
                    }
                }

                net.ser1.stomp.Listener listener = new net.ser1.stomp.Listener() {
                  public void message(Map header, String body) {
                      try {
                          client.prepareIndex("_river", riverName.name())
                              .setSource(body)
                              .execute()
                              .actionGet();
                      } catch(Exception e) {
                          logger.warn("failed to index data [{}]", e, body);
                      }
                  }
                };

                // define the queue
                try {
                    stompClient.subscribe(activemqChannel, listener);
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to subscribe to channel [{}]", e, activemqChannel);
                    }
                    cleanup(0, "failed to subscribe to channel");
                    continue;
                }
            }
        }

        private void cleanup(int code, String message) {
            try {
                logger.debug(message);
                stompClient.unsubscribe(activemqChannel);
                stompClient.disconnect();
            } catch (Exception e) {
                logger.debug("failed to close channel", e);
            }
        }
    }
}
