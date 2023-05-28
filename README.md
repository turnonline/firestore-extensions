# Firestore Extensions

Utilities that extend Firebase Cloud Firestore functionality.

## FirestoreEvent with fault-tolerant methods

The idea behind a set of fault-tolerant methods is that the consumer of the event may not have control over the stored
document structure or types in Firestore.
Therefore, in the case of a value retrieval failure (thrown exception), the automatic re-try mechanism becomes
irrelevant, and the consumer must handle missing or invalid document properties
differently.

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
