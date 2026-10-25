defmodule Prism.TestRedis do
  @moduledoc """
  Redis helpers for tests. The URI follows the app's own `REDIS_URI`, so the
  suite runs against whichever Redis the app under test is using.
  """

  def uri, do: System.get_env("REDIS_URI", "redis://localhost:6379")

  @doc """
  Deletes every key matching `pattern`, walking the keyspace with SCAN so a
  large or shared Redis is never blocked the way KEYS would block it.
  """
  def delete_matching(conn, pattern), do: delete_matching(conn, pattern, "0")

  defp delete_matching(conn, pattern, cursor) do
    [next, keys] = Redix.command!(conn, ["SCAN", cursor, "MATCH", pattern, "COUNT", "500"])
    if keys != [], do: Redix.command!(conn, ["DEL" | keys])
    if next == "0", do: :ok, else: delete_matching(conn, pattern, next)
  end
end
