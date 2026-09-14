defmodule Prism.MetricsAPI do
  @moduledoc """
  Node-local snapshot of Prism's runtime state.

  The snapshot is derived on demand from the processes that own the data rather
  than cached here, so it cannot drift from what the node is doing. It is
  rendered by `Prism.MetricsSink` and available on the metrics endpoint.
  """

  use GenServer

  @name __MODULE__
  @registry :prism_nodes

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: @name)
  end

  @doc """
  Current snapshot of this node.

  Returns the node name, uptime in seconds, the number of active tasks under
  `Prism.TaskSup`, and total BEAM memory in bytes.
  """
  def get_metrics do
    GenServer.call(@name, :get_metrics)
  end

  @impl true
  def init(_) do
    # The :pg default scope is a supervised child of Prism.Supervisor (see
    # Prism.Application), so this only registers the node. Starting the registry
    # from inside this callback — as it used to — left it unsupervised and linked
    # to this GenServer, and silently swallowed `{:error, {:already_started, _}}`
    # on every restart.
    case :pg.join(@registry, self()) do
      :ok -> {:ok, %{start_time: System.system_time(:second)}}
      {:error, reason} -> {:stop, {:registry_join_failed, reason}}
    end
  end

  @impl true
  def handle_call(:get_metrics, _from, state) do
    {:reply, snapshot(state), state}
  end

  defp snapshot(state) do
    %{
      node: node(),
      uptime: System.system_time(:second) - state.start_time,
      active_batches: Supervisor.count_children(Prism.TaskSup).active,
      memory: :erlang.memory(:total)
    }
  end
end
