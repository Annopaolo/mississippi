defmodule Mississippi.Consumer.MessageTracker.Server do
  @moduledoc """
  This module implements the MessageTracker process logic.
  """

  use GenServer, restart: :transient
  require Logger
  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.Message
  alias Mississippi.Consumer.MessageTracker.Server.State
  alias Mississippi.Consumer.MessageTracker.Server.QueueEntry

  @adapter ExRabbitPool.RabbitMQ
  # TODO make this configurable? (Same as DataUpdater)
  @message_tracker_deactivation_interval_ms :timer.hours(3)

  def start_link(args) do
    name = Keyword.fetch!(args, :name)
    _ = Logger.debug("Starting MessageTracker #{inspect(name)}", tag: "message_tracker_start")
    GenServer.start_link(__MODULE__, args, name: name)
  end

  @impl true
  def init(init_args) do
    sharding_key = Keyword.fetch!(init_args, :sharding_key)
    state = %State{queue: :queue.new(), sharding_key: sharding_key, data_updater_pid: nil}
    {:ok, state, @message_tracker_deactivation_interval_ms}
  end

  @impl true
  def handle_cast({:handle_message, %Message{} = message, channel}, state) do
    # :queue.len/1 runs in O(n), :queue.is_empty in O(1)
    if :queue.is_empty(state.queue) do
      new_state = put_message_in_queue(message, channel, state)
      {:noreply, new_state, {:continue, :process_next_message}}
    else
      new_state = put_message_in_queue(message, channel, state)
      {:noreply, new_state, @message_tracker_deactivation_interval_ms}
    end
  end

  @impl true
  def handle_call({:ack_delivery, %Message{} = message}, _from, state) do
    # Invariant: we're always processing the first message in the queue
    # Let us make sure
    case :queue.peek(state.queue) do
      {:value, %QueueEntry{message: ^message, channel: channel}} ->
        @adapter.ack(channel, message.meta.delivery_tag)
        new_state = drop_head_from_queue(state)
        # let's move on to the next message
        {:reply, :ok, new_state, {:continue, :process_next_message}}

      _ ->
        # discard the message and move on to the next
        {:reply, :ok, state, {:continue, :process_next_message}}
    end
  end

  @impl true
  def handle_call({:reject, %Message{} = message}, _from, state) do
    # Invariant: we're always processing the first message in the queue
    # Let us make sure
    case :queue.peek(state.queue) do
      {:value, %QueueEntry{message: ^message, channel: channel}} ->
        @adapter.reject(channel, delivery_tag_from_message(message))
        new_state = drop_head_from_queue(state)
        # let's move on to the next message
        {:reply, :ok, new_state, {:continue, :process_next_message}}

      _ ->
        # discard the message and move on to the next
        {:reply, :ok, state, {:continue, :process_next_message}}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, down_pid, _reason}, state) do
    %State{queue: queue, data_updater_pid: data_updater_pid} = state

    # first of all, let's remove messages from crashed channels/data consumers
    requeued_messages =
      :queue.filter(fn %QueueEntry{} = entry -> entry.channel.pid != down_pid end, queue)

    # Now, let's check if the DataUpdater crashed
    new_dup_pid = if down_pid == data_updater_pid, do: nil, else: data_updater_pid

    new_state = %State{state | queue: requeued_messages, data_updater_pid: new_dup_pid}

    {:noreply, new_state, {:continue, :process_next_message}}
  end

  @impl true
  def handle_info(:timeout, state) do
    {:stop, :normal, state}
  end

  @impl true
  def terminate(reason, _state) do
    _ =
      Logger.info("MessageTracker terminating with reason: #{inspect(reason)}",
        tag: "message_tracker_terminate"
      )

    :ok
  end

  @impl true
  def handle_continue(:process_next_message, state) do
    # We check if there are messages to handle
    if :queue.is_empty(state.queue) do
      # If not, we're ok
      {:noreply, state, @message_tracker_deactivation_interval_ms}
    else
      # otherwise, let's pick the next one...
      %{queue: queue, sharding_key: sharding_key} = state
      {:value, entry} = :queue.peek(queue)
      # ... and tell the DU process to handle it
      {:ok, data_updater_pid} = DataUpdater.get_data_updater_process(sharding_key)
      new_state = maybe_update_and_monitor_dup_pid(state, data_updater_pid)
      DataUpdater.handle_message(data_updater_pid, entry.message)
      {:noreply, new_state, @message_tracker_deactivation_interval_ms}
    end
  end

  defp delivery_tag_from_message(%Message{} = message) do
    message.meta.delivery_tag
  end

  defp put_message_in_queue(message, channel, state) do
    %State{queue: queue} = state

    entry = %QueueEntry{
      channel: channel,
      message: message
    }

    new_queue = :queue.in(entry, queue)
    %State{state | queue: new_queue}
  end

  defp drop_head_from_queue(state) do
    %{queue: queue} = state

    new_queue = :queue.drop(queue)

    %{state | queue: new_queue}
  end

  defp maybe_update_and_monitor_dup_pid(state, new_dup_pid) do
    %State{data_updater_pid: old_data_updater_pid} = state

    if old_data_updater_pid == new_dup_pid do
      state
    else
      Process.monitor(new_dup_pid)
      %State{state | data_updater_pid: new_dup_pid}
    end
  end
end
