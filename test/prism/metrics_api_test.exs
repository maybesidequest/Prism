defmodule Prism.MetricsAPITest do
  use ExUnit.Case, async: false

  test "reports this node's snapshot" do
    metrics = Prism.MetricsAPI.get_metrics()

    assert metrics.node == node()
    assert is_integer(metrics.uptime)
    assert metrics.uptime >= 0
    assert metrics.memory > 0
    assert is_integer(metrics.active_batches)
  end

  test "registers itself in the node registry" do
    assert Process.whereis(Prism.MetricsAPI) in :pg.get_members(:prism_nodes)
  end

  test "the registry is supervised, not owned by the API process" do
    # Regression: the registry used to be started by `:pg.start_link()` inside
    # MetricsAPI.init/1, which left it unsupervised and linked to that GenServer
    # — and re-ran it on every restart.
    assert Process.whereis(:pg) != nil

    {:links, links} = Process.info(Process.whereis(:pg), :links)
    refute Process.whereis(Prism.MetricsAPI) in links
    assert Process.whereis(Prism.Supervisor) in links
  end
end
