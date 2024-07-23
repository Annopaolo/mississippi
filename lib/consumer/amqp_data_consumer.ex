defmodule Mississippi.Consumer.AMQPDataConsumer do
  @moduledoc """
  The AMQPDataConsumer process fetches messages from a Mississippi AMQP queue and
  sends them to MessageTrackers according to the message sharding key.
  """

  require Logger
  use GenServer

  alias AMQP.Channel
  alias Mississippi.Consumer.AMQPDataConsumer.Effects
  alias Mississippi.Consumer.AMQPDataConsumer.State
  alias Mississippi.Consumer.Message
  alias Mississippi.Consumer.MessageTracker

  # TODO should this be customizable?
  @reconnect_interval 1_000
  @adapter ExRabbitPool.RabbitMQ
  @sharding_key "sharding_key"
  @consumer_prefetch_count 300

  # API

  def start_link(args) do
    index = Keyword.fetch!(args, :queue_index)
    GenServer.start_link(__MODULE__, args, name: get_queue_via_tuple(index))
  end

  # Server callbacks

  @impl true
  def init(args) do
    queue_name = Keyword.fetch!(args, :queue_name)

    state = %State{
      queue_name: queue_name,
      monitors: []
    }

    {:ok, state, {:continue, :init_consume}}
  end

  @impl true
  def handle_continue(:init_consume, state), do: init_consume(state)

  @impl true
  def handle_info(:init_consume, state), do: init_consume(state)

  # This is a Message Tracker deactivating itself normally, just remove its monitor.
  # In case a messageTracker crashes, we want to crash too, so that messages are requeued.
  def handle_info(
        {:DOWN, _, :process, pid, :normal},
        %State{channel: %Channel{pid: chan_pid}} = state
      )
      when pid != chan_pid do
    %State{monitors: monitors} = state
    new_monitors = List.delete(monitors, pid)
    {:noreply, %State{state | monitors: new_monitors}}
  end

  # Confirmation sent by the broker after registering this process as a consumer
  def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
  def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  # Confirmation sent by the broker to the consumer process after a Basic.cancel
  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  # Message consumed
  def handle_info({:basic_deliver, payload, meta}, state) do
    %State{channel: channel, monitors: monitors} = state
    {headers, no_headers_meta} = Map.pop(meta, :headers, [])
    headers_map = amqp_headers_to_map(headers)

    {timestamp, clean_meta} = Map.pop(no_headers_meta, :timestamp)

    message = %Message{
      payload: payload,
      headers: headers_map,
      timestamp: timestamp,
      meta: clean_meta
    }

    case message.headers do
      %{@sharding_key => sharding_key_binary} ->
        sharding_key = :erlang.binary_to_term(sharding_key_binary)
        {:ok, mt_pid} = Effects.get_message_tracker(sharding_key)

        new_monitors = maybe_update_monitors(mt_pid, monitors)

        MessageTracker.handle_message(mt_pid, message, channel)
        new_state = %State{state | monitors: new_monitors}
        {:noreply, new_state}

      _ ->
        handle_invalid_msg(message)
        # ACK invalid msg to discard them
        @adapter.ack(channel, meta.delivery_tag)
        {:noreply, state}
    end
  end

  defp maybe_update_monitors(pid, monitors) do
    if pid in monitors do
      monitors
    else
      Process.monitor(pid)
      [pid | monitors]
    end
  end

  defp get_queue_via_tuple(queue_index) when is_integer(queue_index) do
    {:via, Registry, {Registry.AMQPDataConsumer, {:queue_index, queue_index}}}
  end

  defp schedule_connect() do
    Process.send_after(self(), :init_consume, @reconnect_interval)
  end

  defp init_consume(state) do
    conn = Effects.get_connection_worker(:events_consumer_pool)

    case ExRabbitPool.checkout_channel(conn) do
      {:ok, channel} ->
        try_to_setup_consume(channel, conn, state)

      {:error, reason} ->
        _ =
          Logger.warning(
            "Failed to check out channel for consumer on queue #{state.queue_name}: #{inspect(reason)}",
            tag: "channel_checkout_fail"
          )

        schedule_connect()
        {:noreply, state}
    end
  end

  defp try_to_setup_consume(channel, conn, state) do
    %Channel{pid: channel_pid} = channel
    %State{queue_name: queue_name} = state

    with :ok <- @adapter.qos(channel, prefetch_count: @consumer_prefetch_count),
         {:ok, _queue} <- @adapter.declare_queue(channel, queue_name, durable: true),
         {:ok, _consumer_tag} <- @adapter.consume(channel, queue_name, self()) do
      Process.link(channel_pid)

      _ =
        Logger.debug("AMQPDataConsumer for queue #{queue_name} initialized",
          tag: "data_consumer_init_ok"
        )

      {:noreply, %State{state | channel: channel}}
    else
      {:error, reason} ->
        Logger.warning(
          "Error initializing AMQPDataConsumer on queue #{state.queue_name}: #{inspect(reason)}",
          tag: "data_consumer_init_err"
        )

        # Something went wrong, let's put the channel back where it belongs
        _ = ExRabbitPool.checkin_channel(conn, channel)
        schedule_connect()
        {:noreply, %{state | channel: nil}}
    end
  end

  defp handle_invalid_msg(message) do
    %Message{payload: payload, headers: headers, timestamp: timestamp, meta: meta} =
      message

    Logger.warning(
      "Invalid AMQP message: #{inspect(Base.encode64(payload))} #{inspect(headers)} #{inspect(timestamp)} #{inspect(meta)}",
      tag: "data_consumer_invalid_msg"
    )

    :invalid_msg
  end

  defp amqp_headers_to_map(headers) do
    Enum.reduce(headers, %{}, fn {key, _type, value}, acc ->
      Map.put(acc, key, value)
    end)
  end
end

defmodule Mississippi.Consumer.AMQPDataConsumer.Effects do
  use Efx

  alias Mississippi.Consumer.MessageTracker

  @doc false
  @spec get_message_tracker(term()) :: {:ok, pid()}
  defeffect get_message_tracker(sharding_key) do
    MessageTracker.get_message_tracker(sharding_key)
  end

  @doc false
  # returns an exrabbitpool conn
  @spec get_connection_worker(atom()) :: term()
  defeffect get_connection_worker(pool_name) do
    ExRabbitPool.get_connection_worker(:events_consumer_pool)
  end

  @doc false
  @spec checkout_channel(term()) ::
          {:ok, channel :: AMQP.Channel.t()} | {:error, reason :: term()}
  defeffect checkout_channel(conn) do
    ExRabbitPool.checkout_channel(conn)
  end

  @doc false
  @spec checkin_channel(conn :: term(), channel :: AMQP.Channel.t()) :: :ok
  defeffect checkin_channel(conn, channel) do
    ExRabbitPool.checkin_channel(conn, channel)
  end

  defp try_to_setup_consume(channel, conn, state) do
    %Channel{pid: channel_pid} = channel
    %State{queue_name: queue_name} = state

    with :ok <- @adapter.qos(channel, prefetch_count: @consumer_prefetch_count),
         {:ok, _queue} <- @adapter.declare_queue(channel, queue_name, durable: true),
         {:ok, _consumer_tag} <- @adapter.consume(channel, queue_name, self()) do
      Process.link(channel_pid)

      _ =
        Logger.debug("AMQPDataConsumer for queue #{queue_name} initialized",
          tag: "data_consumer_init_ok"
        )

      {:noreply, %State{state | channel: channel}}
    else
      {:error, reason} ->
        Logger.warning(
          "Error initializing AMQPDataConsumer on queue #{state.queue_name}: #{inspect(reason)}",
          tag: "data_consumer_init_err"
        )

        # Something went wrong, let's put the channel back where it belongs
        _ = ExRabbitPool.checkin_channel(conn, channel)
        schedule_connect()
        {:noreply, %{state | channel: nil}}
    end
  end
end
