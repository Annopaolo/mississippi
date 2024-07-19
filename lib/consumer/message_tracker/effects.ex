defmodule Mississippi.Consumer.MessageTracker.Server.Effects do
  @moduledoc """
  Default implementation of Effects used in Mississippi.Consumer.MessageTracker.Server
  """

  use Efx
  require Logger
  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.Message
  alias Mississippi.Consumer.MessageTracker.Server.State

  @spec monitor_channel(AMQP.Channel.t()) :: reference()
  defeffect monitor_channel(channel) do
    Process.monitor(channel.pid)
  end

  @spec handle_message_to_data_updater!(Message.t(), term(), s_1 :: State.t()) :: s_2 :: State.t()
  defeffect handle_message_to_data_updater!(message, sharding_key, state) do
    {:ok, data_updater_pid} = DataUpdater.get_data_updater_process(sharding_key)
    new_state = maybe_update_and_monitor_dup_pid(state, data_updater_pid)
    DataUpdater.handle_message(data_updater_pid, message)
    new_state
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
