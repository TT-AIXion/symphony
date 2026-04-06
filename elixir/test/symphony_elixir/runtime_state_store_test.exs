defmodule SymphonyElixir.RuntimeStateStoreTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RuntimeStateStore

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "symphony-runtime-state-store-#{System.unique_integer([:positive, :monotonic])}"
      )

    File.rm_rf!(root)
    File.mkdir_p!(root)

    previous_log_file = Application.get_env(:symphony_elixir, :log_file)
    previous_state_file = Application.get_env(:symphony_elixir, :runtime_state_file)

    on_exit(fn ->
      restore_env(:log_file, previous_log_file)
      restore_env(:runtime_state_file, previous_state_file)
      File.rm_rf!(root)
    end)

    {:ok, root: root}
  end

  test "default_state_file follows the configured log file root", %{root: root} do
    log_file = Path.join(root, "logs/symphony.log")
    Application.put_env(:symphony_elixir, :log_file, log_file)

    assert RuntimeStateStore.default_state_file() == Path.join(root, "logs/runtime_state.json")
    assert RuntimeStateStore.state_file() == Path.join(root, "logs/runtime_state.json")
  end

  test "load returns an empty snapshot when the file does not exist", %{root: root} do
    Application.put_env(:symphony_elixir, :runtime_state_file, Path.join(root, "missing.json"))

    assert {:ok,
            %{
              running: [],
              retrying: [],
              codex_totals: %{
                "input_tokens" => 0,
                "output_tokens" => 0,
                "total_tokens" => 0,
                "seconds_running" => 0
              },
              rate_limits: nil
            }} = RuntimeStateStore.load()
  end

  test "persist and load round-trip runtime state payloads", %{root: root} do
    state_file = Path.join(root, "runtime_state.json")
    Application.put_env(:symphony_elixir, :runtime_state_file, state_file)

    snapshot = %{
      running: [
        %{
          issue_id: "issue-1",
          identifier: "MT-1",
          started_at: DateTime.utc_now(),
          last_codex_message: %{event: :notification, message: %{method: "thread/tokenUsage/updated"}}
        }
      ],
      retrying: [
        %{
          issue_id: "issue-2",
          attempt: 2,
          due_at_unix_ms: System.system_time(:millisecond) + 1_000
        }
      ],
      codex_totals: %{input_tokens: 1, output_tokens: 2, total_tokens: 3, seconds_running: 4},
      rate_limits: %{"limit_id" => "primary", "primary" => %{"remaining" => 9}}
    }

    assert :ok = RuntimeStateStore.persist(snapshot)

    assert {:ok, loaded} = RuntimeStateStore.load()
    assert loaded.codex_totals == %{"input_tokens" => 1, "output_tokens" => 2, "total_tokens" => 3, "seconds_running" => 4}
    assert loaded.rate_limits == %{"limit_id" => "primary", "primary" => %{"remaining" => 9}}
    assert [%{"issue_id" => "issue-1", "identifier" => "MT-1"}] = loaded.running
    assert [%{"issue_id" => "issue-2", "attempt" => 2}] = loaded.retrying
  end

  test "load reports invalid runtime state json", %{root: root} do
    state_file = Path.join(root, "runtime_state.json")
    Application.put_env(:symphony_elixir, :runtime_state_file, state_file)
    File.write!(state_file, "{not-json")

    assert {:error, {:invalid_runtime_state_json, _reason}} = RuntimeStateStore.load()
  end

  test "load reports invalid runtime state payloads", %{root: root} do
    state_file = Path.join(root, "runtime_state.json")
    Application.put_env(:symphony_elixir, :runtime_state_file, state_file)
    File.write!(state_file, Jason.encode!(%{"no_snapshot" => true}))

    assert {:error, :invalid_runtime_state_payload} = RuntimeStateStore.load()
  end

  test "clear removes the file and ignores missing files", %{root: root} do
    state_file = Path.join(root, "runtime_state.json")
    Application.put_env(:symphony_elixir, :runtime_state_file, state_file)
    File.write!(state_file, "{}")

    assert :ok = RuntimeStateStore.clear()
    refute File.exists?(state_file)
    assert :ok = RuntimeStateStore.clear()
  end

  test "persist logs and returns ok when the target path is invalid", %{root: root} do
    bad_parent = Path.join(root, "not-a-dir")
    File.write!(bad_parent, "file")
    Application.put_env(:symphony_elixir, :runtime_state_file, Path.join(bad_parent, "runtime_state.json"))

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert :ok = RuntimeStateStore.persist(%{running: [], retrying: [], codex_totals: %{}, rate_limits: nil})
      end)

    assert log =~ "Failed to persist runtime state"
  end

  defp restore_env(key, nil), do: Application.delete_env(:symphony_elixir, key)
  defp restore_env(key, value), do: Application.put_env(:symphony_elixir, key, value)
end
