defmodule Mississippi.Consumer.DataUpdater do
  @moduledoc """
  The DataUpdater process takes care of handling messages and signals for a given sharding key.
  Messages are handled using the `message_handler` providedin the Mississippi config,
  which is a module implementing DataUpdater.Handler behaviour.
  Note that the DataUpdater process has no concept of message ordering, as it is the
  MessageTracker process that takes care of maitaining the order of messages.
  """

  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.Message
  require Logger
  use Efx

  @doc """
  Handle a message using the `message_handler` provided in the Mississippi config
  (which is a module implementing DataUpdater.Handler behaviour).
  You can get the DataUpdater instance for a given sharding_key using `get_data_updater_process/1`.
  """
  @spec handle_message(pid(), Message.t()) :: :ok
  defeffect handle_message(data_updater_pid, %Message{} = message) do
    GenServer.cast(data_updater_pid, {:handle_message, message})
  end

  @doc """
  Handles an information that must be forwarded to a `message_handler` but is not a Mississippi message.
  Used to change the state of a stateful Handler. The call is blocking and there is no ordering guarantee.
  You can get the DataUpdater instance for a given sharding_key using `get_data_updater_process/1`.
  """
  @spec handle_signal(pid(), term()) :: term()
  defeffect handle_signal(data_updater_pid, signal) do
    GenServer.call(data_updater_pid, {:handle_signal, signal})
  end

  @doc """
  Provides a reference to the DataUpdater process that will handle the set of messages identified by
  the given sharding key.
  """
  @spec get_data_updater_process(sharding_key :: term()) ::
          {:ok, pid()} | {:error, :data_updater_start_fail}
  defeffect get_data_updater_process(sharding_key) do
    # TODO bring back :offload_start (?)
    case DataUpdater.Supervisor.start_child({DataUpdater.Server, sharding_key: sharding_key}) do
      {:ok, pid} ->
        {:ok, pid}

      {:ok, pid, _info} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      other ->
        _ =
          Logger.warning(
            "Could not start DataUpdater process for sharding_key #{inspect(sharding_key)}: #{inspect(other)}",
            tag: "data_updater_start_fail"
          )

        {:error, :data_updater_start_fail}
    end
  end
end
