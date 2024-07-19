defmodule Mississippi.Consumer.DataUpdater.Handler.TestHandler do
  @behaviour Mississippi.Consumer.DataUpdater.Handler

  @impl true
  def init(sharding_key) do
    {:ok, sharding_key}
  end

  @impl true
  def handle_message(_payload, _headers, _message_id, _timestamp, state) do
    {:ok, :ok, state}
  end

  @impl true
  def handle_signal(_signal, state) do
    {:ok, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end
end
