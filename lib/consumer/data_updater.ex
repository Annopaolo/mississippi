defmodule Mississippi.Consumer.DataUpdater do
  defmodule State do
    defstruct [
      :sharding_key,
      :message_tracker,
      :message_handler,
      :handler_state
    ]
  end

  use GenServer

  alias Mississippi.Consumer.MessageTracker
  alias Mississippi.Consumer.DataUpdater
  require Logger

  # TODO make this configurable?
  @data_updater_deactivation_interval_ms 60 * 60 * 1_000 * 3

  @doc """
  Start handling a message. If it is the first in-order message, it will be processed
  straight away by the `message_handler` provided in the mississippi config
  (which is a module implementing DataUpdater.Handler behaviour).
  If not, the message will remain in memory until it can be processed, i.e. it is now the first
  in-order message.
  """
  def handle_message(sharding_key, payload, headers, tracking_id, timestamp) do
    message_tracker = get_message_tracker(sharding_key)
    {message_id, delivery_tag} = tracking_id
    MessageTracker.track_delivery(message_tracker, message_id, delivery_tag)

    get_data_updater_process(sharding_key)
    |> GenServer.cast({:handle_message, payload, headers, message_id, timestamp})
  end

  @doc """
  Handles an information that must be forwarded to an Handler process,
  but is not a Mississippi message. Used to change the state of
  a stateful Handler. The call is blocking and there is no ordering guarantee.
  """
  def handle_signal(sharding_key, signal) do
    get_data_updater_process(sharding_key)
    |> GenServer.call({:handle_signal, signal})
  end

  @doc """
  Provides a reference to the DataUpdater process that will handle the set of messages identified by
  the given sharding key.
  """
  def get_data_updater_process(sharding_key) do
    # TODO bring back :offload_start (?)
    case DataUpdater.Supervisor.start_child({DataUpdater, sharding_key: sharding_key}) do
      {:ok, pid} ->
        pid

      {:ok, pid, _info} ->
        pid

      {:error, {:already_started, pid}} ->
        pid

      other ->
        _ =
          Logger.warning(
            "Could not start DataUpdater process for sharding_key #{inspect(sharding_key)}: #{inspect(other)}",
            tag: "data_updater_start_fail"
          )

        {:error, :data_updater_start_fail}
    end
  end

  def start_link(extra_args, start_args) do
    {:message_handler, message_handler} = extra_args
    sharding_key = Keyword.fetch!(start_args, :sharding_key)

    init_args = [
      sharding_key: sharding_key,
      message_handler: message_handler
    ]

    name = {:via, Registry, {Registry.DataUpdater, {:sharding_key, sharding_key}}}
    GenServer.start_link(__MODULE__, init_args, name: name)
  end

  @impl true
  def init(init_arg) do
    sharding_key = Keyword.fetch!(init_arg, :sharding_key)
    message_handler = Keyword.fetch!(init_arg, :message_handler)

    with {:ok, handler_state} <- message_handler.init(sharding_key) do
      state = %State{
        sharding_key: sharding_key,
        message_handler: message_handler,
        handler_state: handler_state
      }

      {:ok, state, @data_updater_deactivation_interval_ms}
    end
  end

  @impl true
  def handle_call({:handle_signal, signal}, _from, state) do
    {return_value, new_handler_state} =
      state.message_handler.handle_signal(signal, state.handler_state)

    new_state = %State{state | handler_state: new_handler_state}

    {:reply, return_value, new_state, @data_updater_deactivation_interval_ms}
  end

  @impl true
  def handle_cast({:handle_message, %Message{} = message}, state) do
    %Message{payload: payload, headers: headers, timestamp: timestamp, meta: meta} = message

    case state.message_handler.handle_message(
           payload,
           headers,
           meta.message_id,
           timestamp,
           state.handler_state
         ) do
      {:ok, _, new_handler_state} ->
        _ = Logger.debug("Successfully handled message #{inspect(meta.message_id)}")
        {:ok, message_tracker} = MessageTracker.get_message_tracker(state.sharding_key)
        MessageTracker.ack_delivery(message_tracker, message)

        new_state = %State{state | handler_state: new_handler_state}

        {:noreply, new_state, @data_updater_deactivation_interval_ms}

      {:error, reason, _state} ->
        _ =
          Logger.warning(
            "Error handling message #{inspect(meta.message_id)}, reason #{inspect(reason)}"
          )

        {:ok, message_tracker} = MessageTracker.get_message_tracker(state.sharding_key)
        MessageTracker.reject(message_tracker, message)
        {:noreply, state, @data_updater_deactivation_interval_ms}
    end
  end

  @impl true
  def handle_info({:DOWN, _, :process, _pid, :shutdown}, state) do
    {:stop, :shutdown, state}
  end

  @impl true
  def handle_info({:DOWN, _, :process, _pid, _reason}, state) do
    {:stop, :monitored_process_died, state}
  end

  @impl true
  def handle_info(:timeout, state) do
    {:stop, :normal, state}
  end

  @impl true
  def terminate(reason, state) do
    %State{message_handler: message_handler, handler_state: handler_state} = state
    message_handler.terminate(reason, handler_state)
    :ok
  end
end
