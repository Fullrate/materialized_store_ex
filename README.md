# MaterializedStore

This is a small library for materializing compacted Kafka topics in Elixir
applications.

Given a list of bootstrap Kafka brokers, a topic to read from, and a parser
function, `materialized_store` reads all messages from Kafka, applies the
parser, and saves the results in an ETS table. The resulting key-value store can
then be queried using the `get/2` function.

The provided parser function must take a key and value as read from Kafka(i.e.
strings) and return a tuple to be stored in the key-value store. An example
parser than converts the key to an integer and deserializes the value as a JSON
structure could be:

    fn key, value ->
      {k,""} = Integer.parse(key, 10)
      v = Poison.Parse.parse!(value, keys: :atoms!)
      {k,v}
    end

Kafka supports deletion in compacted topics as well. This happens when the value
is set to a zero-byte string. This is supported as well in `materialized_store`
by returning `nil` for the value. E.g.:

    fn 
      key, value ->
      {k,""} = Integer.parse(key, 10)
      v = case value do
        "" -> nil
        json -> Poison.Parse.parse!(value, keys: atoms!)
      end
      {k,v}
    end

We can also ignore an entry by returning nil for the key. The returned value
will then be ignored. Also note that any exceptions raised during parsing will
be caught and logged to `Logger.debug`.

An example `start_link` invocation with a simple no-op parser could be:

    MaterializedStore.start_link([{'localhost', 9092}], "compacted_topic", fn k,v -> {k,v} end)

Internally, `materialized_store` uses [Brod](https://github.com/klarna/brod) for
all Kafka connectivity and embraces let-it-crash: if the Kafka link crashes, we
simply crash along with it.
