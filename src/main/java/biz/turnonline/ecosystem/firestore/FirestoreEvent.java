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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Firestore event java model with a set of convenient methods to get values from.
 * <p>
 * The idea behind a set of fault-tolerant methods is that the consumer of the event may not have control over
 * the stored document structure or types in Firestore.
 * Therefore, in the case of a value retrieval failure (thrown exception), the automatic re-try mechanism becomes
 * irrelevant, and the consumer must handle missing or invalid document properties
 * differently.
 * </p>
 *
 * @author <a href="mailto:medvegy@turnonline.biz">Aurel Medvegy</a>
 */
public class FirestoreEvent
{
    public static final Logger LOGGER = LoggerFactory.getLogger( FirestoreEvent.class );

    /**
     * Firestore keywords, incomplete list. Missing inner structure: arrayValue and mapValue.
     */
    private static final List<String> FIRESTORE_KEYWORDS = Arrays.asList( "nullValue",
            "booleanValue",
            "integerValue",
            "doubleValue",
            "timestampValue",
            "stringValue",
            "bytesValue",
            "referenceValue",
            "geoPointValue" );

    Value value;

    Value oldValue;

    UpdateMask updateMask;

    /**
     * Returns the boolean indication whether a certain field at a specified path has changed.
     *
     * @param fieldPath the dot separated field path
     * @return true if the field value has changed
     */
    public boolean isUpdated( String fieldPath )
    {
        return updateMask != null && updateMask.fieldPaths != null && updateMask.fieldPaths.contains( fieldPath );
    }

    /**
     * The time when a document was created (the first occurrence).
     * <p>
     * Example
     * </p>
     * <code>2023-02-16T09:49:44.633423Z</code>
     *
     * @return the creation time
     */
    public Date getCreateTime()
    {
        return this.value.createTime;
    }

    /**
     * The time when a document has been updated, the last update.
     * <p>
     * Example
     * </p>
     * <code>2023-03-04T12:37:24.330319Z</code>
     *
     * @return the update time
     */
    public Date getUpdateTime()
    {
        return this.value.updateTime;
    }

    /**
     * The time when a document was created (the first occurrence)
     * <p>
     * Example
     * </p>
     * <code>2023-02-16T09:49:44.633423Z</code>
     *
     * @return the creation time
     */
    public Date getOldCreateTime()
    {
        return this.oldValue.createTime;
    }

    /**
     * The time of the changes, before the last update has occurred.
     * <p>
     * Example
     * </p>
     * <code>2023-02-18T13:42:19.132063Z</code>
     *
     * @return the old update time
     */
    public Date getOldUpdateTime()
    {
        return this.oldValue.updateTime;
    }

    /**
     * Searches the value at the specified property path, or returns {@code null} if it does not exist.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return {@code null} and the incident will be logged as a warning.
     *
     * @param type  the expected type of the response, type of the last path element
     * @param props the property field path elements
     * @return the value taken from the source map or {@code null}
     */
    public <T> T findValueAs( Class<T> type, String... props )
    {
        final Object value = findValueIn( this.value.fields, type, props );
        if ( value == null )
        {
            return null;
        }
        else if ( type.isInstance( value ) )
        {
            return type.cast( value );
        }
        else
        {
            LOGGER.error( "Unexpected response type, "
                    + value.getClass().getName()
                    + " can't be cast to "
                    + type.getName() );
            return null;
        }
    }

    /**
     * <p>
     * Searches the <strong>old</strong> value at the specified property path,
     * or returns {@code null} if it does not exist.
     * Operation is a fault-tolerant, if there is a mismatch between expected type and the current value
     * it will return {@code null} and the incident will be logged as a warning.
     * </p>
     * <p>
     * <strong>OLD values</strong> are present only for update and delete operations,
     * a document object containing a pre-operation document snapshot.
     * </p>
     *
     * @param type  the expected type of the response, type of the last path element
     * @param props the property field path elements
     * @return the value taken from the source map or {@code null}
     */
    public <T> T findOldValueAs( Class<T> type, String... props )
    {
        final Object value = findValueIn( this.oldValue.fields, type, props );
        if ( value == null )
        {
            return null;
        }
        else if ( type.isInstance( value ) )
        {
            return type.cast( value );
        }
        else
        {
            LOGGER.error( "Unexpected response type, "
                    + value.getClass().getName()
                    + " can't be cast to "
                    + type.getName() );
            return null;
        }
    }

    /**
     * Searches the {@link String} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code stringValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<String> findValueAsString( String... props )
    {
        String value = findValueAs( String.class, props );
        return isNullOrEmpty( value ) ? Optional.empty() : Optional.of( value );
    }

    /**
     * Searches the <strong>old</strong> {@link String} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code stringValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<String> findOldValueAsString( String... props )
    {
        String value = findOldValueAs( String.class, props );
        return isNullOrEmpty( value ) ? Optional.empty() : Optional.of( value );
    }

    /**
     * Searches the {@link Boolean} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code booleanValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Boolean> findValueAsBoolean( String... props )
    {
        return Optional.ofNullable( findValueAs( Boolean.class, props ) );
    }

    /**
     * Searches the <strong>old</strong> {@link Boolean} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code booleanValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Boolean> findOldValueAsBoolean( String... props )
    {
        return Optional.ofNullable( findOldValueAs( Boolean.class, props ) );
    }

    /**
     * Searches the {@link Integer} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response, and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code integerValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Integer> findValueAsInteger( String... props )
    {
        return Optional.ofNullable( findValueAs( Integer.class, props ) );
    }

    /**
     * Searches the <strong>old</strong> {@link Integer} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code integerValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Integer> findOldValueAsInteger( String... props )
    {
        return Optional.ofNullable( findOldValueAs( Integer.class, props ) );
    }

    /**
     * Searches the {@link Long} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code integerValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Long> findValueAsLong( String... props )
    {
        return Optional.ofNullable( findValueAs( Long.class, props ) );
    }

    /**
     * Searches the <strong>old</strong> {@link Long} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code integerValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Long> findOldValueAsLong( String... props )
    {
        return Optional.ofNullable( findOldValueAs( Long.class, props ) );
    }

    /**
     * Searches the {@link Double} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code doubleValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Double> findValueAsDouble( String... props )
    {
        return Optional.ofNullable( findValueAs( Double.class, props ) );
    }

    /**
     * Searches the <strong>old</strong> {@link Double} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code doubleValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Double> findOldValueAsDouble( String... props )
    {
        return Optional.ofNullable( findOldValueAs( Double.class, props ) );
    }

    /**
     * Searches the {@link GeoPoint} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code geoPointValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<GeoPoint> findValueAsGeoPoint( String... props )
    {
        return Optional.ofNullable( findValueAs( GeoPoint.class, props ) );
    }

    /**
     * Searches the <strong>old</strong> {@link GeoPoint} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code geoPointValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<GeoPoint> findOldValueAsGeoPoint( String... props )
    {
        return Optional.ofNullable( findOldValueAs( GeoPoint.class, props ) );
    }

    /**
     * Searches the {@link Timestamp} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code timestampValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Timestamp> findValueAsTimestamp( String... props )
    {
        return Optional.ofNullable( findValueAs( Timestamp.class, props ) );
    }

    /**
     * Searches the <strong>old</strong> {@link Timestamp} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code timestampValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Timestamp> findOldValueAsTimestamp( String... props )
    {
        return Optional.ofNullable( findOldValueAs( Timestamp.class, props ) );
    }

    /**
     * Searches the {@link Date} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code timestampValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Date> findValueAsDate( String... props )
    {
        return Optional.ofNullable( findValueAs( Date.class, props ) );
    }

    /**
     * Searches the <strong>old</strong> {@link Date} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code timestampValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Date> findOldValueAsDate( String... props )
    {
        return Optional.ofNullable( findOldValueAs( Date.class, props ) );
    }

    /**
     * Searches the {@link Blob} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code bytesValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Blob> findValueAsBlob( String... props )
    {
        return Optional.ofNullable( findValueAs( Blob.class, props ) );
    }

    /**
     * Searches the <strong>old</strong> {@link Blob} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code bytesValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<Blob> findOldValueAsBlob( String... props )
    {
        return Optional.ofNullable( findOldValueAs( Blob.class, props ) );
    }

    /**
     * Searches the {@link FieldPath} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code referenceValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<FieldPath> findValueAsReference( String... props )
    {
        return Optional.ofNullable( findValueAs( FieldPath.class, props ) );
    }

    /**
     * Searches the <strong>old</strong> {@link FieldPath} value at the specified property path.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return empty response and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code referenceValue}
     * @return the value taken from the source map, or empty response if it does not exist
     */
    public Optional<FieldPath> findOldValueAsReference( String... props )
    {
        return Optional.ofNullable( findOldValueAs( FieldPath.class, props ) );
    }

    /**
     * Searches the {@link List} of values at the specified property path,
     * or returns an empty list if it does not exist.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return an empty list and the incident will be logged as a warning.
     * <p>
     * In case of the unexpected list item type, that value will be missing in the list.
     * </p>
     *
     * @param type  the expected type of the list item
     * @param props the property field path elements, the last one has to be type of {@code arrayValue}
     * @return the list of values taken from the source map or empty list
     */
    @SuppressWarnings( "unchecked" )
    public <T> List<T> findValueAsList( Class<T> type, String... props )
    {
        Object value = findValueIn( this.value.fields, type, props );
        if ( value == null )
        {
            LOGGER.error( "Value at path " + Arrays.toString( props ) + " not found" );
            return Collections.emptyList();
        }
        return ( List<T> ) value;
    }

    /**
     * Searches the <strong>old</strong> {@link List} of values at the specified property path,
     * or returns an empty list if it does not exist.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return an empty list and the incident will be logged as a warning.
     * <p>
     * In case of the unexpected list item type, that value will be missing in the list.
     * </p>
     *
     * @param type  the expected type of the list item
     * @param props the property field path elements, the last one has to be type of {@code arrayValue}
     * @return the list of values taken from the source map or empty list
     */
    @SuppressWarnings( "unchecked" )
    public <T> List<T> findOldValueAsList( Class<T> type, String... props )
    {
        Object value = findValueIn( this.oldValue.fields, type, props );
        if ( value == null )
        {
            LOGGER.error( "Value at path " + Arrays.toString( props ) + " not found" );
            return Collections.emptyList();
        }
        return ( List<T> ) value;
    }

    /**
     * Searches the {@link List} of {@link Map} as values at the specified property path, or returns an empty list
     * if it does not exist.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return an empty list and the incident will be logged as a warning.
     *
     * @param props the property field path elements,
     *              the last one has to be type of {@code arrayValue} followed by {@code mapValue}
     * @return the list of values taken from the source map or empty list
     */
    @SuppressWarnings( "unchecked" )
    public List<Map<String, Object>> findValueAsListOf( String... props )
    {
        Object value = findValueIn( this.value.fields, Map.class, props );
        if ( value == null )
        {
            LOGGER.error( "Value at path " + Arrays.toString( props ) + " not found" );
            return Collections.emptyList();
        }
        return ( List<Map<String, Object>> ) value;
    }

    /**
     * Searches the <strong>old</strong> {@link List} of {@link Map} as values at the specified property path,
     * or returns an empty list if it does not exist.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return an empty list and the incident will be logged as a warning.
     *
     * @param props the property field path elements,
     *              the last one has to be type of {@code arrayValue} followed by {@code mapValue}
     * @return the list of values taken from the source map or empty list
     */
    @SuppressWarnings( "unchecked" )
    public List<Map<String, Object>> findOldValueAsListOf( String... props )
    {
        Object value = findValueIn( this.oldValue.fields, Map.class, props );
        if ( value == null )
        {
            LOGGER.error( "Value at path " + Arrays.toString( props ) + " not found" );
            return Collections.emptyList();
        }
        return ( List<Map<String, Object>> ) value;
    }

    /**
     * Searches the {@link Map} of values at the specified property path, or returns an empty map if it does not exist.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return an empty map and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code mapValue}
     * @return the map of values taken from the source map or empty map
     */
    public Map<String, Object> findValueAsMap( String... props )
    {
        Object value = findValueIn( this.value.fields, Map.class, props );
        return handleValueAsMap( value, props );
    }

    /**
     * Searches the <strong>old</strong> {@link Map} of values at the specified property path, or returns an empty map
     * if it does not exist.
     * Operation is fault-tolerant if there is a mismatch between expected type and the current value,
     * it will return an empty map and the incident will be logged as a warning.
     *
     * @param props the property field path elements, the last one has to be type of {@code mapValue}
     * @return the map of values taken from the source map or empty map
     */
    public Map<String, Object> findOldValueAsMap( String... props )
    {
        Object value = findValueIn( this.oldValue.fields, Map.class, props );
        return handleValueAsMap( value, props );
    }

    @SuppressWarnings( "unchecked" )
    private Map<String, Object> handleValueAsMap( Object value, String[] props )
    {
        if ( value == null )
        {
            LOGGER.error( "Value at path " + Arrays.toString( props ) + " not found" );
            return Collections.emptyMap();
        }
        else if ( !( value instanceof Map ) )
        {
            LOGGER.error( "Value type of the response "
                    + value.getClass().getName()
                    + " is different as expected Map" );
            return Collections.emptyMap();
        }

        return ( Map<String, Object> ) value;
    }

    /**
     * Searches the value at the specified property path, or returns {@code null} if it does not exist
     * (logged as a warning).
     * Operation is a fault-tolerant (except for {@link Map}), if there is a mismatch between expected type
     * and the current value it will return {@code null} and the incident will be logged as a warning.
     * <p>
     * The {@link List} type has a special behavior. If target property represents an 'arrayValue', it will try to
     * convert value in to the target parameter type.
     * In case of the unexpected list item type that value will be missing in the list.
     * So the list might be completely empty.
     * </p>
     * Field value type can be only one of the following:
     * <ul>
     *     <li>nullValue: null</li>
     *     <li>booleanValue: boolean</li>
     *     <li>integerValue: string</li>
     *     <li>doubleValue: double</li>
     *     <li>timestampValue: object {@link Timestamp} or {@link Date}</li>
     *     <li>stringValue: string or {@link Enum}</li>
     *     <li>bytesValue: a base64-encoded string, object {@link Blob}</li>
     *     <li>referenceValue: string or {@link FieldPath}</li>
     *     <li>geoPointValue: object {@link GeoPoint}</li>
     *     <li>arrayValue: object {@link List}</li>
     *     <li>mapValue: object {@link Map}</li>
     * </ul>
     *
     * <strong>Note:</strong> In order to be fault-tolerant for {@link Map} responses too,
     * it's being preferred to use
     * <ul>
     *     <li>{@link #findValueAsMap(String...)} that properly handles the response</li>
     * </ul>
     *
     * @param map   the Firestore map as a source of the values to be searched
     * @param type  the expected type of the response, type of the last path element
     * @param props the property field path elements
     * @return the value taken from the source map or {@code null}
     */
    Object findValueIn( Map<?, ?> map, Class<?> type, String... props )
    {
        Object value = map;
        for ( String pathElement : props )
        {
            if ( value instanceof Map )
            {
                value = ( ( Map<?, ?> ) value ).get( pathElement );
                if ( value instanceof Map )
                {
                    Map<?, ?> mapValue = ( Map<?, ?> ) value;
                    if ( mapValue.keySet().size() == 1 )
                    {
                        if ( FIRESTORE_KEYWORDS.stream().anyMatch( mapValue::containsKey ) )
                        {
                            value = convert( mapValue, type, pathElement );
                        }
                        else
                        {
                            Map<?, ?> mv;
                            if ( mapValue.containsKey( "mapValue" ) )
                            {
                                mv = ( Map<?, ?> ) mapValue.get( "mapValue" );
                            }
                            else
                            {
                                mv = ( Map<?, ?> ) mapValue.get( "arrayValue" );
                            }

                            value = mv == null ? null : mv.values()
                                    .stream()
                                    .findFirst()
                                    .orElse( null );
                        }
                    }
                    else
                    {
                        LOGGER.error( "Unexpected Firestore keywords length " + mapValue.keySet() );
                    }
                }
                if ( value instanceof List )
                {
                    final List<?> listValue = ( List<?> ) value;
                    value = listValue.stream()
                            .map( v -> convert( ( Map<?, ?> ) v, type, pathElement ) )
                            .filter( Objects::nonNull )
                            .collect( Collectors.toList() );
                }
            }
        }

        return value;
    }

    /**
     * Converts Firestore value in to the target type.
     * The map expected to have a single Firestore key from the {@link #FIRESTORE_KEYWORDS}.
     * <p>
     * For unexpected cases it returns {@code null}.
     *
     * @param <T>      the expected result type
     * @param source   the Firestore map with single entry
     * @param type     the expected class type spec
     * @param property the name of the property the value is being evaluated
     * @return the converted value or {@code null}
     */
    private <T> T convert( Map<?, ?> source, Class<T> type, String property )
    {
        final Object value;
        if ( source.containsKey( "nullValue" ) )
        {
            return null;
        }
        else if ( Integer.class.isAssignableFrom( type )
                || int.class.isAssignableFrom( type ) )
        {
            String integerValue = ( String ) source.get( "integerValue" );
            try
            {
                value = integerValue == null ? null : Integer.valueOf( integerValue );
            }
            catch ( NumberFormatException e )
            {
                LOGGER.error( "Value for "
                        + type.getName()
                        + " found, but field is of another type "
                        + source, e );
                return null;
            }
        }
        else if ( Long.class.isAssignableFrom( type )
                || long.class.isAssignableFrom( type ) )
        {
            String integerValue = ( String ) source.get( "integerValue" );
            value = integerValue == null ? null : Long.valueOf( integerValue );
        }
        else if ( Double.class.isAssignableFrom( type )
                || double.class.isAssignableFrom( type ) )
        {
            value = source.get( "doubleValue" );
        }
        else if ( Boolean.class.isAssignableFrom( type )
                || boolean.class.isAssignableFrom( type ) )
        {
            value = source.get( "booleanValue" );
        }
        else if ( String.class.isAssignableFrom( type ) )
        {
            if ( source.containsKey( "referenceValue" ) )
            {
                value = source.get( "referenceValue" );
            }
            else
            {
                value = source.get( "stringValue" );
            }
        }
        else if ( FieldPath.class.isAssignableFrom( type ) )
        {
            String referenceValue = ( String ) source.get( "referenceValue" );
            value = referenceValue == null ? null : FieldPath.of( referenceValue.split( "/" ) );
        }
        else if ( GeoPoint.class.isAssignableFrom( type ) )
        {
            final Object geo = source.get( "geoPointValue" );
            if ( geo != null )
            {
                double latitude = ( double ) ( ( Map<?, ?> ) geo ).get( "latitude" );
                double longitude = ( double ) ( ( Map<?, ?> ) geo ).get( "longitude" );
                value = new GeoPoint( latitude, longitude );
            }
            else
            {
                value = null;
            }
        }
        else if ( Timestamp.class.isAssignableFrom( type ) || Date.class.isAssignableFrom( type ) )
        {
            String timestampValue = ( String ) source.get( "timestampValue" );
            Timestamp timestamp = timestampValue == null ? null : Timestamp.parseTimestamp( timestampValue );
            if ( timestamp != null && Date.class.isAssignableFrom( type ) )
            {
                value = timestamp.toDate();
            }
            else
            {
                value = timestamp;
            }
        }
        else if ( Blob.class.isAssignableFrom( type ) )
        {
            String bytesValue = ( String ) source.get( "bytesValue" );
            if ( bytesValue == null )
            {
                value = null;
            }
            else
            {
                byte[] bytes = Base64.getDecoder().decode( bytesValue );
                value = Blob.fromBytes( bytes );
            }
        }
        else if ( Map.class.isAssignableFrom( type ) )
        {
            Map<?, ?> mv = ( Map<?, ?> ) source.get( "mapValue" );
            value = mv == null ? null : mv.values()
                    .stream()
                    .findFirst()
                    .orElse( null );
        }
        else if ( type.isEnum() )
        {
            String stringValue = ( String ) source.get( "stringValue" );
            try
            {
                //noinspection unchecked,rawtypes
                value = isNullOrEmpty( stringValue ) ? null : Enum.valueOf( ( Class ) type, stringValue );
            }
            catch ( Exception e )
            {
                LOGGER.error( "Value for "
                        + type.getName()
                        + " found, but Enum value is unsupported "
                        + source, e );
                return null;
            }
        }
        else
        {
            value = null;
        }

        if ( value == null && !source.isEmpty() )
        {
            LOGGER.warn( "Value for ["
                    + property
                    + ":"
                    + type.getName()
                    + "] not found, but field has value of another type "
                    + source );
        }

        return type.cast( value );
    }

    /**
     * The boolean indication whether this event represents a newly created document.
     *
     * @return true if this document has been just created
     */
    public boolean isEventTypeCreated()
    {
        return ( this.oldValue.fields == null || this.oldValue.fields.isEmpty() )
                && ( this.updateMask.fieldPaths == null || this.updateMask.fieldPaths.isEmpty() );
    }

    /**
     * The boolean indication whether this event represents an updated document.
     *
     * @return true if this document has been updated
     */
    public boolean isEventTypeUpdated()
    {
        return this.value.fields != null
                && !this.value.fields.isEmpty()
                && this.oldValue.fields != null
                && !this.oldValue.fields.isEmpty();
    }

    /**
     * The boolean indication whether this event represents a deleted document.
     *
     * @return true if this document has been deleted
     */
    public boolean isEventTypeDeleted()
    {
        return ( this.value.fields == null || this.value.fields.isEmpty() )
                && !( this.oldValue.fields == null || this.oldValue.fields.isEmpty() );
    }

    @Override
    public String toString()
    {
        return new StringJoiner( ", ", FirestoreEvent.class.getSimpleName() + "[", "]" )
                .add( "value=" + value )
                .add( "oldValue=" + oldValue )
                .add( "updateMask=" + updateMask )
                .toString();
    }

    public static class Value
    {
        Date createTime;

        Map<String, Object> fields;

        String name;

        Date updateTime;

        @Override
        public String toString()
        {
            return new StringJoiner( ", ", Value.class.getSimpleName() + "[", "]" )
                    .add( "createTime='" + createTime + "'" )
                    .add( "fields='" + fields + "'" )
                    .add( "name='" + name + "'" )
                    .add( "updateTime='" + updateTime + "'" )
                    .toString();
        }
    }

    public static class UpdateMask
    {
        List<String> fieldPaths;

        @Override
        public String toString()
        {
            return new StringJoiner( ", ", UpdateMask.class.getSimpleName() + "[", "]" )
                    .add( "fieldPaths=" + fieldPaths )
                    .toString();
        }
    }
}
