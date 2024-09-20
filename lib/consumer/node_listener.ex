defmodule NodeListener do
  @moduledoc false
  use GenServer

  alias Horde.DynamicSupervisor
  alias Mississippi.Consumer.AMQPDataConsumer

  require Logger

  def start_link(_), do: GenServer.start_link(__MODULE__, [])

  def init(_) do
    :net_kernel.monitor_nodes(true, node_type: :visible)
    {:ok, nil}
  end

  def handle_info({:nodeup, node, node_type}, state) do
    Logger.info("Node #{inspect(node)} of type #{inspect(node_type)} is up")

    # Docs say: "calling this function when supervising a large number of children
    # under low memory conditions can cause an out of memory exception."
    # We assume to have at most hundreds of supervised children and sufficient memory.
    amqp_consumers =
      AMQPDataConsumer.Supervisor
      |> DynamicSupervisor.which_children()
      |> Enum.map(fn {_, pid, _, _} -> pid end)

    # TODO be smarter than this: we can kill only a selected number of
    # consumers in order to re-shard load
    for consumer <- amqp_consumers, consumer != :restarting do
      DynamicSupervisor.terminate_child(AMQPDataConsumer.Supervisor, consumer)
    end

    {:noreply, state}
  end

  def handle_info({:nodedown, node, node_type}, state) do
    Logger.info("Node #{inspect(node)} of type #{inspect(node_type)} is down")

    {:noreply, state}
  end
end
