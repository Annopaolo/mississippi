defmodule Mississippi.Consumer.MessageTracker.Test do
  # We use async so that processes can bind effects even if they're defined in a different process
  # See https://hexdocs.pm/efx/0.1.11/EfxCase.html#module-binding-globally
  use EfxCase, async: false

  alias Mississippi.Consumer.DataUpdater
  alias Mississippi.Consumer.MessageTracker

  require Logger

  setup_all do
    start_supervised!({Registry, [keys: :unique, name: Registry.MessageTracker]})

    start_supervised!(
      {DynamicSupervisor, strategy: :one_for_one, name: MessageTracker.Supervisor}
    )

    :ok
  end

  doctest Mississippi.Consumer.MessageTracker

  test "a MessageTracker process is successfully started with a given sharding key" do
    sharding_key = "sharding_#{System.unique_integer()}"
    {:ok, pid} = MessageTracker.get_message_tracker(sharding_key)

    mt_processes = Registry.select(Registry.MessageTracker, [{{:"$1", :_, :_}, [], [:"$1"]}])

    assert [{:sharding_key, ^sharding_key}] = mt_processes

    on_exit(fn -> DynamicSupervisor.terminate_child(MessageTracker.Supervisor, pid) end)
  end

  test "a MessageTracker process is not duplicated when using the same sharding key" do
    sharding_key = "sharding_#{System.unique_integer()}"
    {:ok, pid} = MessageTracker.get_message_tracker(sharding_key)

    mt_processes = Registry.select(Registry.MessageTracker, [{{:"$1", :_, :_}, [], [:"$1"]}])
    assert [{:sharding_key, ^sharding_key}] = mt_processes

    {:ok, ^pid} = MessageTracker.get_message_tracker(sharding_key)
    assert [{:sharding_key, ^sharding_key}] = mt_processes

    on_exit(fn -> DynamicSupervisor.terminate_child(MessageTracker.Supervisor, pid) end)
  end

  describe "Message roundtrips" do
    setup do
      {:ok, mt_pid} = MessageTracker.get_message_tracker("test_key")

      on_exit(fn -> DynamicSupervisor.terminate_child(MessageTracker.Supervisor, mt_pid) end)

      %{test_process: self(), message_tracker: mt_pid}
    end

    test "when acked", %{test_process: test_process, message_tracker: message_tracker} do
      # mock some DU functions
      bind(
        DataUpdater,
        :get_data_updater_process,
        fn _key ->
          {:ok, test_process}
        end,
        calls: 1
      )

      bind(
        DataUpdater,
        :handle_message,
        fn _du_pid, message ->
          # This function actually runs in the MT process, as it is a like-for-like replacement!
          # We do this to prevent the MT process from `call`ing itself
          Task.async(fn -> MessageTracker.ack_delivery(message_tracker, message) end)
        end,
        calls: 1
      )

      # Use Mox because we don't want to specify a whole %AMQP.Channel{} when using mocks
      mock =
        Mox.expect(MockRabbitMQ, :ack, 1, fn _channel, delivery_tag, _opts ->
          Process.send(test_process, delivery_tag, [])
        end)

      Hammox.allow(mock, self(), message_tracker)

      # Actual test
      in_message = message_fixture()
      MessageTracker.handle_message(message_tracker, in_message, :dontcare)

      assert_receive out_delivery_tag

      assert in_message.meta.delivery_tag == out_delivery_tag
    end
  end

  defp message_fixture(opts \\ []) do
    fields =
      [
        payload: "payload_#{System.unique_integer()}",
        headers: %{},
        timestamp: DateTime.utc_now() |> DateTime.to_unix(),
        meta: %{
          message_id: System.unique_integer(),
          delivery_tag: System.unique_integer()
        }
      ]
      |> Keyword.merge(opts)

    struct!(Mississippi.Consumer.Message, fields)
  end
end
