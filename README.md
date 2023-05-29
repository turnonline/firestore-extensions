# Firestore Extensions

Utilities that extend Firebase Cloud Firestore functionality.

## FirestoreEvent with fault-tolerant methods

The idea behind a set of fault-tolerant methods is that the consumer of the event may not have control over the stored
document structure or types in Firestore.
Therefore, in the case of a value retrieval failure (class cast exception), the automatic re-try mechanism becomes
irrelevant, and the consumer must handle missing or invalid document properties
differently.
This limitation can lead to difficulties in retrieving values reliably, as unexpected missing or invalid data can
disrupt the normal flow of operations.

The extended fault-tolerant methods in Firestore address the challenge of missing or invalid values.
Instead of throwing errors, these methods return Java Optional or null values. This allows consumers to gracefully
handle the absence of data and proceed with their logic effectively.

**Example**

```java
public class FunctionSkeleton
        implements BackgroundFunction<FirestoreEvent>
{
    @Override
    public void accept( FirestoreEvent event, Context context )
    {
        Optional<Long> id = event.findValueAsLong( "id" );
        List<Type> oldTypes = event.findOldValueAsList( Type.class, "types" );
        List<Type> types = event.findValueAsList( Type.class, "types" );
        List<String> list = event.findValueAsList( String.class, "items", "inner", "innerList" );
    }
}
```

For more examples and details,
see the [FirestoreEvent javadoc](src/main/java/biz/turnonline/ecosystem/firestore/FirestoreEvent.java).

## Maven dependency

```xml

<dependency>
    <groupId>biz.turnonline.ecosystem</groupId>
    <artifactId>firestore-extensions</artifactId>
    <version>1.0</version>
</dependency>
```
