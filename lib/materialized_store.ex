defmodule MaterializedStore do
  @moduledoc """
  Materialized Store implements the consumer-side of a Kafka-backed KV store.

  Messages are written to Kafka as (key,val) tuples and read by the store. Each
  message passed through the provided parser, and the resulting, deserialized,
  representation is then stored for retrieval.

  The provided function must satisfy a very simple interface: it is passed the
  key and value as byte strings, as read directly from Kafka, and must return
  a tuple with {key,value} to use in the store. If key is nil the message is
  ignored, if value is nil the old key is deleted from the store. If both key
  and value are set, the value is set for the key in the store.

  The data in the store can be retrieved with the `get` call. The user can wait
  for the store to "synchronize", i.e. catch up to the end of the Kafka topic,
  using the `wait_sync` call. These two operations can be combined with the
  `get_sync` call.

  Note that Materialized Store provides only provides sequential consistency on
  a per-partition basis. This is a result of Kafka providing ordering guarantee
  only for individual partitions. In practice only the per-key ordering will be
  relevant when using Materialized Store.
  """

  use GenServer
  require Logger

  defstruct topic: "",
            parser: nil,
            store: nil,
            seen_offsets: %{},
            unsynchronized: nil,
            waiters: []

  @doc """
  Starts a materialized store.

  The user must provide the following arguments:
  - broker: a list of initial brokers to connect to, of the format `[{'host', port}]`.
  - topic: the topic to consume from. All partitions in the topic will be consumed.
  - parser: a function `(key, val -> {key,val})`

  Examples:

  - `MaterializedStore.start_link([{'localhost', 9092}], "test", fn k,v -> {k,v} end)`

  For return values, and valid options, see `GenServer.start_link`.
  """
  def start_link(broker, topic, parser, options \\ []) do
    GenServer.start_link(__MODULE__, [broker, topic, parser], options)
  end

  @doc """
  Like `start_link`, but returns a child spec rather than starting the store.
  Usually used for running a materialized store in a supervisor tree.
  """
  def child_spec(broker, topic, parser, options \\ []) do
    import Supervisor.Spec
    worker(__MODULE__, [broker, topic, parser], options)
  end

  @doc false
  def init([brokers, topic, parser]) do
    {:ok, cli} = :brod.start_link_client(brokers, :'#{__MODULE__}_#{inspect make_ref}', [])
    {:ok, meta} = :brod_client.get_metadata(cli, topic)
    offsets = spawn_consumer(cli, meta)
    store = :ets.new(:materialized_store, [:set, :protected])
    {:ok, %__MODULE__{topic: topic, parser: parser, store: store, seen_offsets: offsets, unsynchronized: MapSet.new(Map.keys(offsets))}}
  end

  defp spawn_consumer(cli, {:kpro_MetadataResponse, _brokers, [{:kpro_TopicMetadata, :no_error, topic, parts}]}) do
    (for {:kpro_PartitionMetadata, :no_error, part, _leader, _replicas, _isr} <- parts, do: part)
    |> Enum.map(fn p ->
      {:ok, consumer} = :brod_consumer.start_link(cli, topic, p, [], [])
      :ok = :brod_consumer.subscribe(consumer, self, [begin_offset: :earliest])
      {p, -1}
    end)
    |> Enum.into(%{})
  end

  defp synchronized?(state) do
    MapSet.size(state.unsynchronized) == 0
  end

  defp handle_msg({:kafka_message, offset, _, _, _, _}, _part, %{seen_offset: seen} = state) when seen >= offset, do: state
  defp handle_msg({:kafka_message, offset, _, _, key, value, _}, part, state) do
    try do
      # Sanitize inputs
      key = case key do :undefined -> ""; rest -> rest end
      value = case value do :undefined -> ""; rest -> rest end
      case state.parser.(key, value) do
        {nil,_} -> nil
        {k, nil} -> :ets.delete(state.store, k)
        {k, v} -> :ets.insert(state.store, {k,v})
      end
    rescue
      e -> Logger.error(fn -> "Message parsing failed for input message at #{state.topic}:#{part} @ #{offset}: #{inspect e}" end, offset: offset)
    end
    %{state | seen_offsets: Map.put(state.seen_offsets, part, offset)}
  end

  defp mark_synchronized(state, part) do
    state = %{state | unsynchronized: MapSet.delete(state.unsynchronized, part)}
    case synchronized?(state) do
      false -> state
      true -> for w <- state.waiters, do: GenServer.reply(w, :ok)
        state = %{state | waiters: []}
    end
  end

  @doc false
  def handle_info({consumer, {:kafka_message_set, _topic, part, hwm, msgs}}, state) do
    state = msgs |> Enum.reduce(state, &(handle_msg(&1, part, &2)))
    :brod_consumer.ack(consumer, state.seen_offsets[part])
    lag = hwm - state.seen_offsets[part] - 1
    state = case lag do 0 -> mark_synchronized(state, part); _ -> state end
    {:noreply, state}
  end


  @doc false
  def handle_call(:get_table, _from, state) do
    {:reply, state.store, state}
  end
  def handle_call({:get, key}, _from, state) do
    case :ets.lookup(state.store, key) do
      [{_k, val}] -> {:reply, val, state}
      _ -> {:reply, nil, state}
    end
  end

  @doc false
  def handle_call(:wait_sync, from, state) do
    if synchronized?(state) do
      {:reply, :ok, state}
    else
      {:noreply, %{state | waiters: [from | state.waiters]}}
    end
  end

  # Public API

  @doc """
  Retrieve the underlying ETS table. The table can be queried directly if
  maximum performance is needed. The semantics on-read are the same as for
  `get`.
  """
  def get_table(pid, timeout \\ 5000) do
    GenServer.call(pid, :get_table, timeout)
  end

  @doc """
  Retrieves the value for the given key, or nil if the key does not exist.
  """
  def get(pid, key, timeout \\ 5000) do
    GenServer.call(pid, {:get, key}, timeout)
  end

  @doc """
  Waits for the materialized store to catch up to the head of the Kafka topic.
  Until the head has been reached, stale reads may be served.
  """
  def wait_sync(pid, timeout \\ 30000) do
    GenServer.call(pid, :wait_sync, timeout)
  end

  @doc """
  Combines wait_sync and get as a single call. Note that internally this simply
  calls `wait_sync` followed by `get`, and thus exists purely as a helper.
  """
  def get_sync(pid, key, timeout \\ 30000) do
    wait_sync(pid, timeout)
    get(pid, key, timeout)
  end
end
