defmodule Mississippi.Consumer.AMQPDataConsumer.State do
  use TypedStruct

  typedstruct do
    field :queue_name, String.t(), enforce: true
    field :channel, term()
  end
end
