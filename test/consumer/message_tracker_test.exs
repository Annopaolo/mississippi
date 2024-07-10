defmodule Mississippi.Consumer.MessageTracker.Test do
  use EfxCase

  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.MessageTracker

  setup_all do
    start_supervised!({Registry, [keys: :unique, name: Registry.MessageTracker]})

    start_supervised!(
      {DynamicSupervisor, strategy: :one_for_one, name: MessageTracker.Supervisor}
    )

    :ok
  end

  doctest Mississippi.Consumer.MessageTracker

  # test "a MessageTracker process is successfully started with a given sharding key" do
  #   sharding_key = "sharding_#{System.unique_integer()}"
  #   {:ok, pid} = MessageTracker.get_message_tracker(sharding_key)

  #   dup_processes = Registry.select(Registry.MessageTracker, [{{:"$1", :_, :_}, [], [:"$1"]}])

  #   assert [{:sharding_key, ^sharding_key}] = dup_processes

  #   on_exit(fn -> DynamicSupervisor.terminate_child(MessageTracker.Supervisor, pid) end)
  # end

  # test "a MessageTracker process is not duplicated when using the same sharding key" do
  #   sharding_key = "sharding_#{System.unique_integer()}"
  #   {:ok, pid} = MessageTracker.get_message_tracker(sharding_key)

  #   dup_processes = Registry.select(Registry.MessageTracker, [{{:"$1", :_, :_}, [], [:"$1"]}])
  #   assert [{:sharding_key, ^sharding_key}] = dup_processes

  #   {:ok, ^pid} = MessageTracker.get_message_tracker(sharding_key)
  #   assert [{:sharding_key, ^sharding_key}] = dup_processes

  #   on_exit(fn -> DynamicSupervisor.terminate_child(MessageTracker.Supervisor, pid) end)
  # end

  setup do
    start_supervised!({Registry, [keys: :unique, name: Registry.DataUpdater]})

    start_supervised!({DataUpdater.Supervisor, message_handler: DataUpdater.Handler.Impl})

    {:ok, pid} = DataUpdater.get_data_updater_process("test_key")

    %{dup_pid: pid}
  end

  test "let's try something", %{dup_pid: dup_pid} do
    # start a message tracker
    {:ok, mt_pid} = MessageTracker.get_message_tracker("test_key")
    in_message = message_fixture()
    MessageTracker.handle_message(mt_pid, in_message, :dont_care_of_channel_for_now)

    # mock some DU functions
    bind(DataUpdater, :get_data_updater_process, fn _key -> {:ok, dup_pid} end)

    bind(DataUpdater, :handle_message, fn _dup_pid, message ->
      MessageTracker.ack_delivery(mt_pid, message)
    end)

    # Real test (?)
    bind(MessageTracker, :ack_delivery, fn _mt_pid, out_message ->
      assert out_message == in_message
    end)
  end

  defp message_fixture(opts \\ []) do
    fields =
      [
        payload: "payload_#{System.unique_integer()}",
        headers: %{},
        timestamp: DateTime.utc_now(),
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
