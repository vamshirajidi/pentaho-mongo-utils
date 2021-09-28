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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import org.bson.json.JsonParseException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;


class MongoPropToOption {
  private MongoUtilLogger log;

  MongoPropToOption( MongoUtilLogger log ) {
    this.log = log;
  }

  public int intValue( String value, int defaultVal ) {
    if ( !Util.isEmpty( value ) ) {
      try {
        return Integer.parseInt( value );
      } catch ( NumberFormatException n ) {
        logWarn(
          BaseMessages.getString( PKG, "MongoPropToOption.Warning.Message.NumberFormat", value, Integer.toString( defaultVal ) ) );
        return defaultVal;
      }
    }
    return defaultVal;
  }

  public long longValue( String value, long defaultVal ) {
    if ( !Util.isEmpty( value ) ) {
      try {
        return Long.parseLong( value );
      } catch ( NumberFormatException n ) {
        logWarn(
          BaseMessages.getString( PKG, "MongoPropToOption.Warning.Message.NumberFormat", value, Long.toString( defaultVal ) ) );
        return defaultVal;
      }
    }
    return defaultVal;
  }

  public boolean boolValue( String value, boolean defaultVal ) {
    if ( !Util.isEmpty( value ) ) {
      return Boolean.parseBoolean( value );
    }
    return defaultVal;
  }

  private static Class<?> PKG = MongoPropToOption.class;

  public ReadPreference readPrefValue( MongoProperties props ) throws MongoDbException {
    String readPreference = props.get( MongoProp.readPreference );
    if ( Util.isEmpty( readPreference ) ) {
      // nothing to do
      return null;
    }
    DBObject[] tagSets;
    try {
      tagSets = getTagSets( props );
    } catch ( JsonParseException e) {
      throw new MongoDbException(e.getMessage());
    }

    NamedReadPreference preference = NamedReadPreference.byName( readPreference );
    if ( preference == null ) {
      throw new MongoDbException(
        BaseMessages.getString( PKG, "MongoPropToOption.ErrorMessage.ReadPreferenceNotFound", readPreference,
          getPrettyListOfValidPreferences() ) );
    }
    logInfo( BaseMessages.getString(
        PKG, "MongoPropToOption.Message.UsingReadPreference", preference.getName() ) );

    if ( preference == NamedReadPreference.PRIMARY && tagSets.length > 0 ) {
      // Invalid combination.  Tag sets are not used with PRIMARY
      logWarn( BaseMessages.getString(
          PKG, "MongoPropToOption.Message.Warning.PrimaryReadPrefWithTagSets" ) );
      return preference.getPreference();
    } else if ( tagSets.length > 0 ) {
      logInfo(
        BaseMessages.getString(
              PKG, "MongoPropToOption.Message.UsingReadPreferenceTagSets",
              Arrays.toString( tagSets ) ) );
      DBObject[] remainder = tagSets.length > 1 ? Arrays.copyOfRange( tagSets, 1, tagSets.length ) : new DBObject[ 0 ];
      return preference.getTaggableReadPreference( tagSets[0], remainder );
    } else {
      logInfo( BaseMessages.getString( PKG, "MongoPropToOption.Message.NoReadPreferenceTagSetsDefined" ) );
      return preference.getPreference();
    }
  }

  private String getPrettyListOfValidPreferences() {
    // [primary, primaryPreferred, secondary, secondaryPreferred, nearest]
    return Arrays.toString( new ArrayList<String>( NamedReadPreference.getPreferenceNames() ).toArray() );
  }

  DBObject[] getTagSets( MongoProperties props ) throws JsonParseException {
    String tagSet = props.get( MongoProp.tagSet );
    if ( tagSet != null ) {
      BasicDBList list;
      if ( !tagSet.trim().startsWith( "[" ) ) {
        // wrap the set in an array
        tagSet = "[" + tagSet + "]";
      }

      BasicDBObject tagSetObject = BasicDBObject.parse( String.format( "{\"tagset\" : %s }", tagSet ) );
      list = (BasicDBList) tagSetObject.get( "tagset" );

      return list.toArray(new DBObject[list.size()]);
    }

    return new DBObject[0];
  }

  // Changing WriteConcern Constructor according to the documentation mentioned at following link
  // http://mongodb.github.io/mongo-java-driver/3.2.0/javadoc/com/mongodb/WriteConcern.html
  //
  //  journaled: If true block until write operations have been committed to the journal. Cannot be used in combination
  //  with fsync. Prior to MongoDB 2.6 this option was ignored if the server was running without journaling.Starting
  //  with MongoDB 2.6 write operations will fail with an exception if this option is used when the server is running
  //  without journaling.
  //
  //  fsync: If true and the server is running without journaling, blocks until the server has synced all data files
  //  to disk. If the server is running with journaling, this acts the same as the j option, blocking until write
  //  operations have been committed to the journal. Cannot be used in combination with j. In almost all cases the j
  //  flag should be used in preference to this one.

  public WriteConcern writeConcernValue( final MongoProperties props )
    throws MongoDbException {
    // write concern
    String writeConcern = props.get( MongoProp.writeConcern );
    String wTimeout = props.get( MongoProp.wTimeout );
    boolean journaled = Boolean.valueOf( props.get( MongoProp.JOURNALED ) );

    WriteConcern concern;

    if ( !Util.isEmpty( writeConcern ) && Util.isEmpty( wTimeout ) && !journaled ) {
      // all defaults - timeout 0, journal = false, w = 1
      concern = new WriteConcern( 1 );

      if ( log != null ) {
        log.info(
          BaseMessages.getString( PKG, "MongoPropToOption.Message.ConfiguringWithDefaultWriteConcern" ) ); //$NON-NLS-1$
      }
    } else {
      int wt = 0;
      if ( !Util.isEmpty( wTimeout ) ) {
        try {
          wt = Integer.parseInt( wTimeout );
        } catch ( NumberFormatException n ) {
          throw new MongoDbException( n );
        }
      }

      if ( !Util.isEmpty( writeConcern ) ) {
        // try parsing as a number first
        try {
          int wc = Integer.parseInt( writeConcern );
          concern = new WriteConcern( wc, wt );
          concern.withJournal( journaled );
        } catch ( NumberFormatException n ) {
          // assume its a valid string - e.g. "majority" or a custom
          // getLastError label associated with a tag set
          concern = new WriteConcern( writeConcern );
          concern.withWTimeout( wt, TimeUnit.MILLISECONDS );
          concern.withJournal( journaled );
        }
      } else {
        concern = new WriteConcern( 1, wt );
        concern.withJournal( journaled );
      }

      if ( log != null ) {
        String lwc =
          "w = " + concern.getWObject() + ", wTimeout = " + concern.getWTimeout( TimeUnit.MILLISECONDS ) + ", journaled = "
            + concern.getJournal();
        log.info( BaseMessages.getString( PKG, "MongoPropToOption.Message.ConfiguringWithWriteConcern", lwc ) );
      }
    }
    return concern;
  }

  private void logInfo( String message ) {
    if ( log != null ) {
      log.info( message );
    }
  }

  private void logWarn( String message ) {
    if ( log != null ) {
      log.warn( message, null );
    }
  }
}
