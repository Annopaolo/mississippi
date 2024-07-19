defmodule Mississippi.Consumer.DataUpdater.Test do
  use ExUnit.Case
  import Hammox

  alias Mississippi.Consumer.DataUpdater

  setup_all do
    start_supervised!({Registry, [keys: :unique, name: Registry.DataUpdater]})

    start_supervised!({DataUpdater.Supervisor, message_handler: DataUpdater.Handler.Impl})

    :ok
  end

  doctest Mississippi.Consumer.DataUpdater

  test "a DataUpdater process is successfully started with a given sharding key" do
    sharding_key = "sharding_#{System.unique_integer()}"
    {:ok, pid} = DataUpdater.get_data_updater_process(sharding_key)

    dup_processes = Registry.select(Registry.DataUpdater, [{{:"$1", :_, :_}, [], [:"$1"]}])

    assert [{:sharding_key, ^sharding_key}] = dup_processes

    on_exit(fn -> DataUpdater.Supervisor.terminate_child(pid) end)
  end

  test "a DataUpdater process is not duplicated when using the same sharding key" do
    sharding_key = "sharding_#{System.unique_integer()}"
    {:ok, pid} = DataUpdater.get_data_updater_process(sharding_key)

    dup_processes = Registry.select(Registry.DataUpdater, [{{:"$1", :_, :_}, [], [:"$1"]}])
    assert [{:sharding_key, ^sharding_key}] = dup_processes

    {:ok, ^pid} = DataUpdater.get_data_updater_process(sharding_key)
    assert [{:sharding_key, ^sharding_key}] = dup_processes

    on_exit(fn -> DataUpdater.Supervisor.terminate_child(pid) end)
  end

  describe "The DataUpdater process" do
    setup do
      start_supervised!({Registry, [keys: :unique, name: Registry.MessageTracker]})

      start_supervised!(
        {DynamicSupervisor, strategy: :one_for_one, name: Consumer.MessageTracker.Supervisor}
      )

      sharding_key = "sharding_#{System.unique_integer()}"
      {:ok, pid} = DataUpdater.get_data_updater_process(sharding_key)
      on_exit(fn -> DataUpdater.Supervisor.terminate_child(pid) end)
      %{dup_pid: pid}
    end

    test "successfully handles message", %{dup_pid: dup_pid} do
      mock =
        MockMessageHandler
        |> expect(:handle_message, 1, fn _, _, _, _, state ->
          {:ok, :ok, state}
        end)

      Hammox.allow(mock, self(), dup_pid)

      DataUpdater.handle_message(dup_pid, message_fixture())
    end

    test "successfully handles signal", %{dup_pid: dup_pid} do
      mock =
        MockMessageHandler
        |> expect(:handle_signal, 1, fn _, state -> {:ok, state} end)

      Hammox.allow(mock, self(), dup_pid)

      DataUpdater.handle_signal(dup_pid, signal_fixture())
    end
  end

  defp message_fixture(opts \\ []) do
    fields =
      [
        payload: "payload_#{System.unique_integer()}",
        headers: %{},
        timestamp: DateTime.utc_now() |> DateTime.to_unix(),
        meta: %{
          message_id: System.unique_integer()
        }
      ]
      |> Keyword.merge(opts)

    struct!(Mississippi.Consumer.Message, fields)
  end

  defp signal_fixture(opts \\ []) do
    Keyword.get(opts, :signal, "signal_#{System.unique_integer()}")
  end
end
