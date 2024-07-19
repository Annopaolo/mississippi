defmodule Mississippi.Consumer.DataUpdater.Effects do
  @moduledoc """
  Default implementation of Effects used in Mississippi.Consumer.DataUpdater
  """

  use Efx
  require Logger
  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.MessageTracker

  @spec get_data_updater_process(term()) :: {:ok, pid()} | {:error, :data_updater_start_fail}
  defeffect get_data_updater_process(sharding_key) do
    case DataUpdater.Supervisor.start_child({DataUpdater, sharding_key: sharding_key}) do
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

  @spec ack_message!(Message.t(), term()) :: :ok
  defeffect ack_message!(message, sharding_key) do
    {:ok, message_tracker} = MessageTracker.get_message_tracker(sharding_key)
    MessageTracker.ack_delivery(message_tracker, message)
  end

  @spec reject_message!(Message.t(), term()) :: :ok
  defeffect reject_message!(message, sharding_key) do
    {:ok, message_tracker} = MessageTracker.get_message_tracker(sharding_key)
    MessageTracker.reject(message_tracker, message)
  end
end
