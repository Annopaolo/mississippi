defmodule Mississippi.Consumer.MessageTracker.Server.QueueEntry do
  use TypedStruct

  typedstruct do
    field :channel, term(), enforce: true

    field :message, term(), enforce: true
  end
end
