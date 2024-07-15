defmodule Mississippi.Consumer.AMQPDataConsumer do
  @moduledoc """
  The AMQPDataConsumer process fetches messages from a Mississippi AMQP queue and
  sends them to MessageTrackers according to the message sharding key.
  """

  require Logger
  use GenServer

  alias AMQP.Channel
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
      queue_name: queue_name
    }

    {:ok, state, {:continue, :init_consume}}
  end

  @impl true
  def handle_continue(:init_consume, state), do: init_consume(state)

  @impl true
  def handle_call({:ack, delivery_tag}, _from, %State{channel: chan} = state) do
    res = @adapter.ack(chan, delivery_tag)
    {:reply, res, state}
  end

  def handle_call({:reject, delivery_tag}, _from, %State{channel: chan} = state) do
    res = @adapter.reject(chan, delivery_tag, requeue: false)
    {:reply, res, state}
  end

  def handle_call({:requeue, delivery_tag}, _from, %State{channel: chan} = state) do
    res = @adapter.reject(chan, delivery_tag, requeue: true)
    {:reply, res, state}
  end

  @impl true
  def handle_info(:init_consume, state), do: init_consume(state)

  # This is a Message Tracker deactivating itself normally, do nothing.
  # In case a messageTracker crashes, we want to crash too, so that messages are requeued.
  def handle_info(
        {:DOWN, _, :process, pid, :normal},
        %State{channel: %Channel{pid: chan_pid}} = state
      )
      when pid != chan_pid do
    {:noreply, state}
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
    %State{channel: chan} = state
    {headers, no_headers_meta} = Map.pop(meta, :headers, [])
    headers_map = amqp_headers_to_map(headers)

    {timestamp, clean_meta} = Map.pop(no_headers_meta, :timestamp)

    message = %Message{
      payload: payload,
      headers: headers_map,
      timestamp: timestamp,
      meta: clean_meta
    }

    case handle_consume(message, chan) do
      :ok ->
        :ok

      :invalid_msg ->
        # ACK invalid msg to discard them
        @adapter.ack(chan, meta.delivery_tag)
    end

    {:noreply, state}
  end

  defp get_queue_via_tuple(queue_index) when is_integer(queue_index) do
    {:via, Registry, {Registry.AMQPDataConsumer, {:queue_index, queue_index}}}
  end

  defp schedule_connect() do
    Process.send_after(self(), :init_consume, @reconnect_interval)
  end

  defp init_consume(state) do
    conn = ExRabbitPool.get_connection_worker(:events_consumer_pool)

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

  defp handle_consume(%Message{} = message, %Channel{} = channel) do
    with %{@sharding_key => sharding_key_binary} <- message.headers do
      sharding_key = :erlang.binary_to_term(sharding_key_binary)

      {:ok, pid} = MessageTracker.get_message_tracker(sharding_key)

      MessageTracker.handle_message(pid, message, channel)
    else
      _ -> handle_invalid_msg(message)
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
