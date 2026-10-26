%{
  configs: [
    %{
      name: "default",
      files: %{
        included: ["lib/", "test/", "config/"],
        excluded: [~r"/_build/", ~r"/deps/"]
      },
      checks: %{
        extra: [
          # Discord error codes (10008, 10015, ...) read better as written in its docs.
          {Credo.Check.Readability.LargeNumbers, only_greater_than: 99_999}
        ],
        disabled: [
          # The delivery path (DiscordWorker, HTTP, FanoutBroadway.Batch) is a few
          # long functions; splitting them is its own change. Until then these
          # three checks would only repeat that.
          {Credo.Check.Refactor.CyclomaticComplexity, []},
          {Credo.Check.Refactor.Nesting, []},
          {Credo.Check.Refactor.FunctionArity, []}
        ]
      }
    }
  ]
}
