# Schema-Typed CEL

Builds CEL environments that understand your schemas. Catches type errors at compile time instead of runtime.

## The problem

Without schema typing, CEL accepts anything:

```go
// compiles, explodes at runtime
"event.totally_fake_field > 100"
// same story
"event.amount > 'not a number'"  
```

With schema typing:

```
compile error: undefined field 'totally_fake_field' on type 'Order'
compile error: no matching overload for '>' with (double, string)
```

Find out at startup.

## Usage

```go
// Parse schema (Avro or JSON Schema)
mapper := schema.NewAvroMapper() 
mapped, _ := mapper.Map([]byte(schemaJSON))

// Build typed environment
adapter, _ := schema.NewAvroAdapter(mapped)
provider := schema.NewTypeProvider()
env, _, _ := schema.BuildTypedEnv(adapter, provider, schema.DefaultEnvOptions())

// Compile with type checking
ast, issues := env.Compile("event.amount > 100.0 && event.status == 'confirmed'")
```

## Supported schemas

**Avro** - records, arrays, maps, unions, enums, logical types (timestamps, dates, UUIDs)

**JSON Schema** - objects, arrays, `$ref`, `allOf`, `anyOf`, `oneOf`, `enum`, `const`, formats (`date-time`, `email`, etc.)

## Type mappings

| Schema Type | CEL Type |
|-------------|----------|
| string | `string` |
| int/integer/long | `int` |
| float/double/number | `double` |
| boolean | `bool` |
| bytes/binary | `bytes` |
| array | `list(T)` |
| map/additionalProperties | `map(string, V)` |
| record/object | named object type |
| timestamp/date-time | `timestamp` |
| duration/time | `duration` |

## Envelope fields

Standard fields available in all expressions:

```cel
_event_id      // bytes
_event_type    // string
_occurred_at   // timestamp
_ingested_at   // timestamp
_schema_id     // int
_lsn           // int
```

## Nested access

```cel
event.customer.address.city == "NYC"
event.items[0].price > 10.0
event.metadata["source"] == "web"
```
