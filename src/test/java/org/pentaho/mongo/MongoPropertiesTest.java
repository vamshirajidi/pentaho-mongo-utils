/*!
* Copyright 2010 - 2021 Hitachi Vantara.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package org.pentaho.mongo;

import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

public class MongoPropertiesTest {
  @Test
  public void testBuildsMongoClientOptions() throws Exception {
    MongoProperties props = new MongoProperties.Builder()
        .set( MongoProp.connectionsPerHost, "127" )
        .set( MongoProp.connectTimeout, "333" )
        .set( MongoProp.maxWaitTime, "12345" )
        .set( MongoProp.cursorFinalizerEnabled, "false" )
        .set( MongoProp.socketTimeout, "4" )
        .set( MongoProp.useSSL, "true" )
        .set( MongoProp.readPreference, "primary" )
        .set( MongoProp.USE_KERBEROS, "false" )
        .set( MongoProp.USE_ALL_REPLICA_SET_MEMBERS, "false" )
        .build();
    MongoUtilLogger log = Mockito.mock( MongoUtilLogger.class );
    MongoClientOptions options = props.buildMongoClientOptions( log );
    assertEquals( 127, options.getConnectionsPerHost() );
    assertEquals( 333, options.getConnectTimeout() );
    assertEquals( 12345, options.getMaxWaitTime() );
    assertFalse( options.isCursorFinalizerEnabled() );
    assertEquals( 4, options.getSocketTimeout() );
    assertTrue( options.isSslEnabled() );
    assertEquals( options.getReadPreference(), ReadPreference.primary() );
    assertEquals( props.getReadPreference(), ReadPreference.primary() );
    assertFalse( props.useAllReplicaSetMembers() );
    assertFalse( props.useKerberos() );
    assertEquals( "MongoProperties:\n"
        + "connectionsPerHost=127\n"
        + "connectTimeout=333\n"
        + "cursorFinalizerEnabled=false\n"
        + "HOST=localhost\n"
        + "maxWaitTime=12345\n"
        + "PASSWORD=\n"
        + "readPreference=primary\n"
        + "socketTimeout=4\n"
        + "USE_ALL_REPLICA_SET_MEMBERS=false\n"
        + "USE_KERBEROS=false\n"
        + "useSSL=true\n", props.toString() );
  }

  @Test
  public void testBuildsMongoClientOptionsDefaults() throws Exception {
    MongoProperties props = new MongoProperties.Builder().build();
    MongoUtilLogger log = Mockito.mock( MongoUtilLogger.class );
    MongoClientOptions options = props.buildMongoClientOptions( log );
    assertEquals( 100, options.getConnectionsPerHost() );
    assertEquals( 10000, options.getConnectTimeout() );
    assertEquals( 120000, options.getMaxWaitTime() );
    assertTrue( options.isCursorFinalizerEnabled() );
    assertEquals( 0, options.getSocketTimeout() );
    assertFalse( options.isSslEnabled() );
  }
}
