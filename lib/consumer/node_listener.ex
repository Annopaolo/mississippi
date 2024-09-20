defmodule NodeListener do
  @moduledoc false
  use GenServer

  require Logger

  def start_link(_), do: GenServer.start_link(__MODULE__, [])

  def init(_) do
    :net_kernel.monitor_nodes(true, node_type: :visible)
    {:ok, nil}
  end

  def handle_info({:nodeup, node, node_type}, state) do
    _ = Logger.info("Node #{inspect(node)} of type #{inspect(node_type)} is up")
    {:noreply, state}
  end

  def handle_info({:nodedown, node, node_type}, state) do
    _ = Logger.info("Node #{inspect(node)} of type #{inspect(node_type)} is down")
    {:noreply, state}
  end
end
