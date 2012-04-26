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

import net.ser1.stomp.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class ActiveMQRiverTest {

    public static void main(String[] args) throws Exception {

        /* Building the elasticsearch node */
        Node node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("gateway.type", "none")).node();

        node.client().prepareIndex("_river", "test1", "_meta").setSource(jsonBuilder().startObject().field("type", "activemq").endObject()).execute().actionGet();

        /* Connecting to queue */
        Client stomp_client = new Client("localhost", 61613, "guest", "guest" );

        String message = "{ \"index\" : { \"index\" : \"test\", \"type\" : \"type1\", \"id\" : \"1\" }\n" +
                         "{ \"type1\" : { \"field1\" : \"value1\" } }\n" +
                         "{ \"delete\" : { \"index\" : \"test\", \"type\" : \"type1\", \"id\" : \"2\" } }\n" +
                         "{ \"create\" : { \"index\" : \"test\", \"type\" : \"type1\", \"id\" : \"1\" }\n" +
                         "{ \"type1\" : { \"field1\" : \"value1\" } }";

        stomp_client.send("/elasticsearch", message);


        Thread.sleep(100000);
    }
}
