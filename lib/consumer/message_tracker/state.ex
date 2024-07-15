defmodule Mississippi.Consumer.MessageTracker.Server.State do
  use TypedStruct

  typedstruct do
    field :queue, term(), enforce: true

    field :data_updater_pid, pid()

    field :sharding_key, term(), enforce: true
  end
end
