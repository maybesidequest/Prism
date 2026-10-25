defmodule Prism.CancelCheckerTest do
  use ExUnit.Case, async: false

  alias Prism.CancelChecker

  setup do
    {:ok, redix} = Redix.start_link(Prism.TestRedis.uri(), sync_connect: true)
    %{redix: redix}
  end

  describe "cancelled?/1" do
    test "returns true when cancel key exists", %{redix: redix} do
      message_id = "test_cancel_msg_#{System.unique_integer()}"
      Redix.command!(redix, ["SET", "prism:cancel:#{message_id}", "1"])

      assert CancelChecker.cancelled?(message_id) == true

      Redix.command!(redix, ["DEL", "prism:cancel:#{message_id}"])
    end

    test "returns false when cancel key does not exist" do
      message_id = "nonexistent_#{System.unique_integer()}"
      assert CancelChecker.cancelled?(message_id) == false
    end

    test "returns false (fails open) when called with empty string" do
      assert CancelChecker.cancelled?("") == false
    end
  end
end
