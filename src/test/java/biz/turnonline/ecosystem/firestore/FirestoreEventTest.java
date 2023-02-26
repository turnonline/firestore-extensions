/*
 * Copyright (c) 2023 TurnOnline.biz s.r.o. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package biz.turnonline.ecosystem.firestore;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.GeoPoint;
import com.google.gson.Gson;
import org.junit.jupiter.api.Test;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * {@link FirestoreEvent} unit testing.
 *
 * @author <a href="mailto:medvegy@turnonline.biz">Aurel Medvegy</a>
 */

class FirestoreEventTest
{
    private final FirestoreEvent testedDocument = readPayloadFrom( "firestore-document.json" );

    private final FirestoreEvent testedBytes = readPayloadFrom( "firestore-document-bytes.json" );

    private final FirestoreEvent deleted = readPayloadFrom( "firestore-document-deleted.json" );

    /**
     * Reads the content of the file in the same package as this test and converts it into Firestore event payload.
     *
     * @return the name of the JSON file
     */
    static FirestoreEvent readPayloadFrom( String jsonFile )
    {
        InputStream stream = FirestoreEventTest.class.getResourceAsStream( jsonFile );
        if ( stream == null )
        {
            throw new IllegalArgumentException( jsonFile + " not found" );
        }

        String json = new BufferedReader( new InputStreamReader( stream ) )
                .lines()
                .collect( Collectors.joining( System.lineSeparator() ) );
        return new Gson().fromJson( json, FirestoreEvent.class );
    }

    @Test
    void findValueAs_HappyScenarios_Null()
    {
        String nValue = testedDocument.findValueAs( String.class, "nFieldNull" );
        assertWithMessage( "Null value" )
                .that( nValue )
                .isNull();
    }

    @Test
    void findValueAs_HappyScenarios_AsString()
    {
        String sValue = testedDocument.findValueAs( String.class, "aFieldString" );
        assertWithMessage( "Value of type String" )
                .that( sValue )
                .isEqualTo( "String value" );

        assertWithMessage( "Value of type String" )
                .that( testedDocument.findValueAsString( "aFieldString" ).orElse( null ) )
                .isEqualTo( sValue );
    }

    @Test
    void findOldValueAs_HappyScenarios_AsString()
    {
        String sValue = testedDocument.findOldValueAs( String.class, "aFieldString" );
        assertWithMessage( "Value of type String" )
                .that( sValue )
                .isEqualTo( "String value" );

        assertWithMessage( "Value of type String" )
                .that( testedDocument.findOldValueAsString( "aFieldString" ).orElse( null ) )
                .isEqualTo( sValue );
    }

    @Test
    void findValueAs_NotFound_AsString()
    {
        assertWithMessage( "Value of type String is empty" )
                .that( testedDocument.findValueAsString( "unknown" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_InvalidType_AsString()
    {
        assertWithMessage( "Value of type String is empty" )
                .that( testedDocument.findValueAsString( "bFieldBoolean" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_HappyScenarios_AsBoolean()
    {
        Boolean bValue = testedDocument.findValueAs( Boolean.class, "bFieldBoolean" );
        assertWithMessage( "Value of type Boolean" )
                .that( bValue )
                .isTrue();

        assertWithMessage( "Value of type Boolean" )
                .that( testedDocument.findValueAsBoolean( "bFieldBoolean" ).orElse( false ) )
                .isTrue();
    }

    @Test
    void findOldValueAs_HappyScenarios_AsBoolean()
    {
        assertWithMessage( "Value of type Boolean" )
                .that( testedDocument.findOldValueAsBoolean( "bFieldBoolean" ).orElse( false ) )
                .isTrue();
    }

    @Test
    void findValueAs_NotFound_AsBoolean()
    {
        assertWithMessage( "Value of type Boolean is empty" )
                .that( testedDocument.findValueAsBoolean( "unknown" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_InvalidType_AsBoolean()
    {
        assertWithMessage( "Value of type Boolean is empty" )
                .that( testedDocument.findValueAsBoolean( "eFieldInteger" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_HappyScenarios_AsInteger()
    {
        Integer iValue = testedDocument.findValueAs( Integer.class, "eFieldInteger" );
        assertWithMessage( "Value of type Integer" )
                .that( iValue )
                .isEqualTo( 852456 );

        assertWithMessage( "Value of type Integer" )
                .that( testedDocument.findValueAsInteger( "eFieldInteger" ).orElse( null ) )
                .isEqualTo( iValue );
    }

    @Test
    void findOldValueAs_HappyScenarios_AsInteger()
    {
        assertWithMessage( "Value of type Integer" )
                .that( testedDocument.findOldValueAsInteger( "eFieldInteger" ).orElse( null ) )
                .isEqualTo( 852456 );
    }

    @Test
    void findValueAs_NotFound_AsInteger()
    {
        assertWithMessage( "Value of type Integer is empty" )
                .that( testedDocument.findValueAsInteger( "unknown" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_InvalidType_AsInteger()
    {
        assertWithMessage( "Value of type Integer is empty" )
                .that( testedDocument.findValueAsInteger( "aFieldString" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_HappyScenarios_AsLong()
    {
        Long lValue = testedDocument.findValueAs( Long.class, "cFieldLong" );
        assertWithMessage( "Value of type Long" )
                .that( lValue )
                .isEqualTo( 2747879507027928L );

        assertWithMessage( "Value of type Long" )
                .that( testedDocument.findValueAsLong( "cFieldLong" ).orElse( null ) )
                .isEqualTo( lValue );
    }

    @Test
    void findOldValueAs_HappyScenarios_AsLong()
    {
        assertWithMessage( "Value of type Long" )
                .that( testedDocument.findOldValueAsLong( "cFieldLong" ).orElse( null ) )
                .isEqualTo( 9810877456027117L );
    }

    @Test
    void findValueAs_HappyScenarios_IntegerAsLong()
    {
        Long lValue = testedDocument.findValueAs( Long.class, "eFieldInteger" );
        assertWithMessage( "Value of type Long" )
                .that( lValue )
                .isEqualTo( 852456L );
    }

    @Test
    void findValueAs_InvalidType_LongAsInteger()
    {
        Optional<Integer> lValue = testedDocument.findValueAsInteger( "cFieldLong" );
        assertWithMessage( "Value of type Long is empty" )
                .that( lValue.isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_NotFound_AsLong()
    {
        assertWithMessage( "Value of type Long is empty" )
                .that( testedDocument.findValueAsLong( "unknown" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_InvalidType_AsLong()
    {
        assertWithMessage( "Value of type Long is empty" )
                .that( testedDocument.findValueAsLong( "dFieldDouble" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_HappyScenarios_AsDouble()
    {
        Double dValue = testedDocument.findValueAs( Double.class, "dFieldDouble" );
        assertWithMessage( "Value of type Double" )
                .that( dValue )
                .isEqualTo( 199.85 );

        assertWithMessage( "Value of type Double" )
                .that( testedDocument.findValueAsDouble( "dFieldDouble" ).orElse( null ) )
                .isEqualTo( dValue );
    }

    @Test
    void findOldValueAs_HappyScenarios_AsDouble()
    {
        assertWithMessage( "Value of type Double" )
                .that( testedDocument.findOldValueAsDouble( "dFieldDouble" ).orElse( null ) )
                .isEqualTo( 178.99 );
    }

    @Test
    void findValueAs_NotFound_AsDouble()
    {
        assertWithMessage( "Value of type Double is empty" )
                .that( testedDocument.findValueAsDouble( "unknown" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_InvalidType_AsDouble()
    {
        assertWithMessage( "Value of type Double" )
                .that( testedDocument.findValueAsDouble( "gFieldGeoPoint" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_HappyScenarios_AsGeoPoint()
    {
        GeoPoint gValue = testedDocument.findValueAs( GeoPoint.class, "gFieldGeoPoint" );
        assertWithMessage( "Value of type GeoPoint Latitude" )
                .that( gValue.getLatitude() )
                .isEqualTo( 48.50217 );

        assertWithMessage( "Value of type GeoPoint Longitude" )
                .that( gValue.getLongitude() )
                .isEqualTo( 16.9704 );

        Optional<GeoPoint> again = testedDocument.findValueAsGeoPoint( "gFieldGeoPoint" );
        assertWithMessage( "Value of type GeoPoint is present" )
                .that( again.isPresent() )
                .isTrue();

        assertWithMessage( "Value of type GeoPoint Latitude" )
                .that( again.get().getLatitude() )
                .isEqualTo( gValue.getLatitude() );

        assertWithMessage( "Value of type GeoPoint Longitude" )
                .that( again.get().getLongitude() )
                .isEqualTo( gValue.getLongitude() );
    }

    @Test
    void findOldValueAs_HappyScenarios_AsGeoPoint()
    {
        Optional<GeoPoint> gValue = testedDocument.findOldValueAsGeoPoint( "gFieldGeoPoint" );
        assertWithMessage( "Value of type GeoPoint is present" )
                .that( gValue.isPresent() )
                .isTrue();

        assertWithMessage( "Value of type GeoPoint Latitude" )
                .that( gValue.get().getLatitude() )
                .isEqualTo( 48.50217 );

        assertWithMessage( "Value of type GeoPoint Longitude" )
                .that( gValue.get().getLongitude() )
                .isEqualTo( 16.9704 );
    }

    @Test
    void findValueAs_NotFound_AsGeoPoint()
    {
        Optional<GeoPoint> gValue = testedDocument.findValueAsGeoPoint( "unknown" );
        assertWithMessage( "Value of type GeoPoint is empty" )
                .that( gValue.isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_InvalidType_AsGeoPoint()
    {
        Optional<GeoPoint> gValue = testedDocument.findValueAsGeoPoint( "listOf" );
        assertWithMessage( "Value of type GeoPoint is empty" )
                .that( gValue.isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_HappyScenarios_AsTimestamp()
    {
        Timestamp tsValue = testedDocument.findValueAs( Timestamp.class, "tFieldTimestamp" );
        assertWithMessage( "Value of type Timestamp" )
                .that( tsValue.getSeconds() )
                .isEqualTo( 1676540885 );
        assertWithMessage( "Value of type Timestamp" )
                .that( tsValue.getNanos() )
                .isEqualTo( 735000000 );

        Optional<Timestamp> again = testedDocument.findValueAsTimestamp( "tFieldTimestamp" );
        assertWithMessage( "Value of type Timestamp is present" )
                .that( again.isPresent() )
                .isTrue();

        assertWithMessage( "Value of type Timestamp" )
                .that( again.get().getSeconds() )
                .isEqualTo( tsValue.getSeconds() );
        assertWithMessage( "Value of type Timestamp" )
                .that( again.get().getNanos() )
                .isEqualTo( tsValue.getNanos() );
    }

    @Test
    void findOldValueAs_HappyScenarios_AsTimestamp()
    {
        Optional<Timestamp> again = testedDocument.findOldValueAsTimestamp( "tFieldTimestamp" );
        assertWithMessage( "Value of type Timestamp is present" )
                .that( again.isPresent() )
                .isTrue();

        assertWithMessage( "Value of type Timestamp" )
                .that( again.get().getSeconds() )
                .isEqualTo( 1676540885 );
        assertWithMessage( "Value of type Timestamp" )
                .that( again.get().getNanos() )
                .isEqualTo( 735000000 );
    }

    @Test
    void findValueAs_NotFound_AsTimestamp()
    {
        Optional<Timestamp> tsValue = testedDocument.findValueAsTimestamp( "unknown" );
        assertWithMessage( "Value of type Timestamp is empty" )
                .that( tsValue.isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_InvalidType_AsTimestamp()
    {
        Optional<Timestamp> tsValue = testedDocument.findValueAsTimestamp( "rFieldReference" );
        assertWithMessage( "Value of type Timestamp is empty" )
                .that( tsValue.isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_HappyScenarios_AsDate()
    {
        Date dValue = testedDocument.findValueAs( Date.class, "tFieldTimestamp" );
        assertWithMessage( "Value of type Timestamp" )
                .that( dValue )
                .isEqualTo( new Date( 1676540885735L ) );

        assertWithMessage( "Value of type Timestamp" )
                .that( testedDocument.findValueAsDate( "tFieldTimestamp" ).orElse( null ) )
                .isEqualTo( dValue );
    }

    @Test
    void findOldValueAs_HappyScenarios_AsDate()
    {
        assertWithMessage( "Value of type Timestamp" )
                .that( testedDocument.findOldValueAsDate( "tFieldTimestamp" ).orElse( null ) )
                .isEqualTo( new Date( 1676540885735L ) );
    }

    @Test
    void findValueAs_NotFound_AsDate()
    {
        Optional<Date> dValue = testedDocument.findValueAsDate( "unknown" );
        assertWithMessage( "Value of type Timestamp is empty" )
                .that( dValue.isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_InvalidType_AsDate()
    {
        Optional<Date> dValue = testedDocument.findValueAsDate( "aFieldString" );
        assertWithMessage( "Value of type Timestamp is empty" )
                .that( dValue.isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_HappyScenarios_AsBlob() throws IOException
    {
        Blob blobValue = testedBytes.findValueAs( Blob.class, "baFieldImage" );
        assertWithMessage( "Value of type Blob" )
                .that( blobValue )
                .isNotNull();

        BufferedImage image = ImageIO.read( new ByteArrayInputStream( blobValue.toBytes() ) );

        assertWithMessage( "Value of type Blob, image" )
                .that( image.getRaster().getHeight() )
                .isEqualTo( 92 );

        assertWithMessage( "Value of type Blob, image" )
                .that( image.getRaster().getWidth() )
                .isEqualTo( 125 );

        Optional<Blob> againBlob = testedBytes.findValueAsBlob( "baFieldImage" );

        assertWithMessage( "Value of type Blob is present" )
                .that( againBlob.isPresent() )
                .isTrue();

        BufferedImage again = ImageIO.read( new ByteArrayInputStream( againBlob.get().toBytes() ) );
        assertWithMessage( "Value of type Blob, image" )
                .that( again.getRaster().getHeight() )
                .isEqualTo( image.getRaster().getHeight() );

        assertWithMessage( "Value of type Blob, image" )
                .that( image.getRaster().getWidth() )
                .isEqualTo( image.getRaster().getWidth() );

    }

    @Test
    void findOldValueAs_HappyScenarios_AsBlob()
    {
        Optional<Blob> blobValue = testedBytes.findOldValueAsBlob( "baFieldImage" );

        assertWithMessage( "Value of type Blob is present" )
                .that( blobValue.isPresent() )
                .isFalse();
    }

    @Test
    void findValueAs_NotFound_AsBlob()
    {
        Optional<Blob> blobValue = testedBytes.findValueAsBlob( "unknownProperty" );
        assertWithMessage( "Value of type Blob is empty" )
                .that( blobValue.isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_InvalidType_AsBlob()
    {
        Optional<Blob> blobValue = testedBytes.findValueAsBlob( "iField" );
        assertWithMessage( "Value of type Blob is empty" )
                .that( blobValue.isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_HappyScenarios_AsReference()
    {
        FieldPath refValue = testedDocument.findValueAs( FieldPath.class, "rFieldReference" );
        assertWithMessage( "Value of type FieldPath (Firestore full document path)" )
                .that( refValue.toString() )
                .isEqualTo( "projects.`prj-1ab`.databases.`(default)`.documents.offers.`9jpnp0GakiAIHNEsdkFg`" );

        final Optional<FieldPath> refValue2 = testedDocument.findValueAsReference( "rFieldReference" );
        assertWithMessage( "Value of type FieldPath is present" )
                .that( refValue2.isPresent() )
                .isTrue();

        assertWithMessage( "Value of type FieldPath (Firestore full document path)" )
                .that( refValue2.get().toString() )
                .isEqualTo( refValue.toString() );

        String stringRef = testedDocument.findValueAs( String.class, "rFieldReference" );
        assertWithMessage( "Value of type String (Firestore full document path)" )
                .that( stringRef )
                .isEqualTo( "projects/prj-1ab/databases/(default)/documents/offers/9jpnp0GakiAIHNEsdkFg" );
    }

    @Test
    void findOldValueAs_HappyScenarios_AsReference()
    {
        Optional<FieldPath> refValue = testedDocument.findOldValueAsReference( "rFieldReference" );
        assertWithMessage( "Value of type FieldPath is present" )
                .that( refValue.isPresent() )
                .isTrue();

        assertWithMessage( "Value of type FieldPath (Firestore full document path)" )
                .that( refValue.get().toString() )
                .isEqualTo( "projects.`prj-1ab`.databases.`(default)`.documents.offers.`9jpnp0GakiAIHNEsdkFg`" );
    }

    @Test
    void findValueAs_NotFound_AsReference()
    {
        assertWithMessage( "Value of type Reference  is empty" )
                .that( testedDocument.findValueAsReference( "unknown" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_InvalidType_AsReference()
    {
        assertWithMessage( "Value of type Reference  is empty" )
                .that( testedDocument.findValueAsReference( "cFieldLong" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_HappyScenarios_AsInnerMapProperties()
    {
        Boolean bValue = testedDocument.findValueAs( Boolean.class, "map", "b-property-2" );
        assertWithMessage( "Value of type Boolean (inner map structure)" )
                .that( bValue )
                .isTrue();

        String sValue = testedDocument.findValueAs( String.class, "map", "s-property-1" );
        assertWithMessage( "Value of type String (inner map structure)" )
                .that( sValue )
                .isEqualTo( "String value for s-property-1" );

        Timestamp tsValue = testedDocument.findValueAs( Timestamp.class, "map", "tPropertyX" );
        assertWithMessage( "Value of type Timestamp" )
                .that( tsValue.getSeconds() )
                .isEqualTo( 1673773238 );
        assertWithMessage( "Value of type Timestamp" )
                .that( tsValue.getNanos() )
                .isEqualTo( 23000000 );

        Integer iValue = testedDocument.findValueAs( Integer.class, "items", "inner", "inStock" );
        assertWithMessage( "Value of type Integer (inner map structure)" )
                .that( iValue )
                .isEqualTo( 1852 );
    }

    @Test
    void findValueAs_HappyScenarios_AsList()
    {
        List<String> list = testedDocument.findValueAsList( String.class, "items", "inner", "innerList" );
        assertWithMessage( "Value of type List<String>" )
                .that( list )
                .hasSize( 4 );

        assertWithMessage( "List item[0] value" )
                .that( list.get( 0 ) )
                .isEqualTo( "Inner list item 1" );

        assertWithMessage( "List item[1] value" )
                .that( list.get( 1 ) )
                .isEqualTo( "Inner list item 2" );

        assertWithMessage( "List item[2] value" )
                .that( list.get( 2 ) )
                .isEqualTo( "Inner list item 3" );

        assertWithMessage( "List item[3] value" )
                .that( list.get( 3 ) )
                .isEqualTo( "Inner list item 4" );

        List<Timestamp> listOf = testedDocument.findValueAsList( Timestamp.class, "listOf" );
        assertWithMessage( "Value of type List<?>" )
                .that( listOf )
                .hasSize( 3 );

        assertWithMessage( "List item[0] value type" )
                .that( listOf.get( 0 ) )
                .isInstanceOf( Timestamp.class );

        assertWithMessage( "List item[1] value type" )
                .that( listOf.get( 1 ) )
                .isInstanceOf( Timestamp.class );
    }

    @Test
    void findOldValueAs_HappyScenarios_AsList()
    {
        List<Timestamp> list = testedDocument.findOldValueAsList( Timestamp.class, "listOf" );
        assertWithMessage( "Value of type List<?>" )
                .that( list )
                .hasSize( 3 );

        assertWithMessage( "List item[0] value type" )
                .that( list.get( 0 ) )
                .isInstanceOf( Timestamp.class );

        assertWithMessage( "List item[1] value type" )
                .that( list.get( 1 ) )
                .isInstanceOf( Timestamp.class );

        assertWithMessage( "List item[2] value type" )
                .that( list.get( 2 ) )
                .isInstanceOf( Timestamp.class );
    }

    @Test
    void findOldValueAs_NotFound_AsList()
    {
        List<String> list = testedDocument.findOldValueAsList( String.class, "unknown" );
        assertWithMessage( "Value of type List<String>" )
                .that( list )
                .isEmpty();
    }

    @Test
    void findValueAs_InvalidType_AsList()
    {
        List<Long> list = testedDocument.findValueAsList( Long.class, "items", "inner", "innerList" );
        assertWithMessage( "Invalid property list type" )
                .that( list )
                .isEmpty();

        List<Float> anotherList = testedDocument.findValueAsList( Float.class, "listOf" );
        assertWithMessage( "Unsupported property list type" )
                .that( anotherList )
                .isEmpty();
    }

    @Test
    void findValueAs_NotFound_AsList()
    {
        List<Long> list = testedDocument.findValueAsList( Long.class, "unknown" );
        assertWithMessage( "Unknown property list" )
                .that( list )
                .isEmpty();
    }

    @Test
    void findValueAs_HappyScenarios_AsListOfMap()
    {
        List<Map<String, Object>> list = testedDocument.findValueAsListOf( "listOfMap" );
        assertWithMessage( "List of map items" )
                .that( list )
                .hasSize( 2 );

        Map<String, Object> first = list.get( 0 );
        assertWithMessage( "First map item keys" )
                .that( first.keySet() )
                .containsExactly( "p1", "p2", "p3", "p4", "p5" );

        Map<String, Object> second = list.get( 1 );
        assertWithMessage( "First map item keys" )
                .that( second.keySet() )
                .containsExactly( "date", "name", "numberOf" );

        // map chaining value retrieval tests
        Object p3 = testedDocument.findValueIn( first, String.class, "p3" );
        assertWithMessage( "numberOf property value" )
                .that( p3 )
                .isEqualTo( "Value for P3" );

        Object numberOf = testedDocument.findValueIn( second, Integer.class, "numberOf" );
        assertWithMessage( "numberOf property value" )
                .that( numberOf )
                .isEqualTo( 105 );
    }

    @Test
    void findOldValueAs_NotFound_AsListOfMap()
    {
        List<Map<String, Object>> list = testedDocument.findOldValueAsListOf( "listOfMap" );
        assertWithMessage( "List of map items" )
                .that( list )
                .isEmpty();
    }

    @Test
    void findValueAs_NotFound_AsListOfMap()
    {
        List<Map<String, Object>> list = testedDocument.findValueAsListOf( "unknown" );
        assertWithMessage( "List of map items" )
                .that( list )
                .isEmpty();
    }

    @Test
    void findValueAs_InvalidType_AsListOfMap()
    {
        List<Map<String, Object>> list = testedDocument.findValueAsListOf( "listOf" );
        assertWithMessage( "List of map items" )
                .that( list )
                .isEmpty();
    }

    @Test
    void findValueAs_HappyScenarios_AsMap()
    {
        Map<String, Object> map = testedDocument.findValueAsMap( "items", "inner" );
        assertWithMessage( "Property map" )
                .that( map )
                .hasSize( 3 );

        assertWithMessage( "Property map" )
                .that( map )
                .containsKey( "InnerFieldName" );

        assertWithMessage( "Property map" )
                .that( map )
                .containsKey( "innerList" );

        assertWithMessage( "Property map" )
                .that( map )
                .containsKey( "inStock" );

        Map<String, Object> another = testedDocument.findValueAsMap( "map" );
        assertWithMessage( "Property map" )
                .that( another )
                .hasSize( 4 );

        assertWithMessage( "Property map" )
                .that( another )
                .containsKey( "s-property-1" );

        assertWithMessage( "Property map" )
                .that( another )
                .containsKey( "b-property-2" );

        assertWithMessage( "Property map" )
                .that( another )
                .containsKey( "tPropertyX" );

        assertWithMessage( "Property map" )
                .that( another )
                .containsKey( "lastName" );
    }

    @Test
    void findOldValueAs_HappyScenarios_AsMap()
    {
        Map<String, Object> map = testedDocument.findOldValueAsMap( "map" );
        assertWithMessage( "Property map" )
                .that( map )
                .hasSize( 3 );

        assertWithMessage( "Property map" )
                .that( map )
                .containsKey( "s-property-1" );

        assertWithMessage( "Property map" )
                .that( map )
                .containsKey( "b-property-2" );

        assertWithMessage( "Property map" )
                .that( map )
                .containsKey( "tPropertyX" );
    }

    @Test
    void findValueAs_NotFound_AsMap()
    {
        Map<String, Object> map = testedDocument.findValueAsMap( "unknown" );
        assertWithMessage( "Property map" )
                .that( map )
                .isEmpty();
    }

    @Test
    void findValueAs_InvalidType_AsMap()
    {
        Map<String, Object> map = testedDocument.findValueAsMap( "listOf" );
        assertWithMessage( "Property map" )
                .that( map )
                .isEmpty();
    }

    @Test
    void findValueAs_NotFound_AnyProperty()
    {
        assertWithMessage( "Value of type String is empty" )
                .that( testedDocument.findValueAsString( "blaBla" ).isEmpty() )
                .isTrue();
    }

    @Test
    void findValueAs_UnsupportedType()
    {
        Object response = testedDocument.findValueAs( Byte.class, "eFieldInteger" );
        assertWithMessage( "Unsupported property type" )
                .that( response )
                .isNull();
    }

    @Test
    void isEventTypeCreated()
    {
        assertWithMessage( "Document created" )
                .that( testedDocument.isEventTypeCreated() )
                .isFalse();

        assertWithMessage( "Document created" )
                .that( testedBytes.isEventTypeCreated() )
                .isTrue();

        assertWithMessage( "Document updated" )
                .that( deleted.isEventTypeCreated() )
                .isFalse();
    }

    @Test
    void isEventTypeUpdated()
    {
        assertWithMessage( "Document updated" )
                .that( testedDocument.isEventTypeUpdated() )
                .isTrue();

        assertWithMessage( "Document updated" )
                .that( testedBytes.isEventTypeUpdated() )
                .isFalse();

        assertWithMessage( "Document updated" )
                .that( deleted.isEventTypeUpdated() )
                .isFalse();
    }

    @Test
    void isEventTypeDeleted()
    {
        assertWithMessage( "Document deleted" )
                .that( testedDocument.isEventTypeDeleted() )
                .isFalse();

        assertWithMessage( "Document deleted" )
                .that( testedBytes.isEventTypeDeleted() )
                .isFalse();

        assertWithMessage( "Document deleted" )
                .that( deleted.isEventTypeDeleted() )
                .isTrue();
    }
}