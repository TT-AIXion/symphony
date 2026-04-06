defmodule SymphonyElixir.RuntimeStateStore do
  @moduledoc false

  require Logger

  alias SymphonyElixir.LogFile

  @state_file_name "runtime_state.json"
  @state_version 1

  @type snapshot :: %{
          running: [map()],
          retrying: [map()],
          codex_totals: map(),
          rate_limits: map() | nil
        }

  @spec state_file() :: Path.t()
  def state_file do
    Application.get_env(:symphony_elixir, :runtime_state_file) ||
      default_state_file()
  end

  @spec load() :: {:ok, snapshot()} | {:error, term()}
  def load do
    case File.read(state_file()) do
      {:ok, body} ->
        decode_snapshot(body)

      {:error, :enoent} ->
        {:ok, empty_snapshot()}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec persist(snapshot()) :: :ok
  def persist(%{} = snapshot) do
    path = Path.expand(state_file())
    tmp_path = path <> ".tmp"

    try do
      :ok = File.mkdir_p(Path.dirname(path))

      payload =
        %{
          "version" => @state_version,
          "saved_at" => DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601(),
          "snapshot" => normalize_value(snapshot)
        }
        |> Jason.encode_to_iodata!(pretty: true)

      :ok = File.write(tmp_path, payload)
      :ok = File.rename(tmp_path, path)
    rescue
      error ->
        Logger.warning("Failed to persist runtime state: #{Exception.message(error)}")
        :ok
    after
      _ = File.rm(tmp_path)
    end
  end

  @spec clear() :: :ok
  def clear do
    case File.rm(state_file()) do
      :ok ->
        :ok

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to clear runtime state file: #{inspect(reason)}")
        :ok
    end
  end

  @spec default_state_file() :: Path.t()
  def default_state_file do
    log_file = Application.get_env(:symphony_elixir, :log_file, LogFile.default_log_file())

    log_file
    |> Path.expand()
    |> Path.dirname()
    |> Path.join(@state_file_name)
  end

  defp decode_snapshot(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, %{"snapshot" => snapshot}} when is_map(snapshot) ->
        {:ok,
         %{
           running: list_value(snapshot["running"]),
           retrying: list_value(snapshot["retrying"]),
           codex_totals: map_value(snapshot["codex_totals"]),
           rate_limits: optional_map_value(snapshot["rate_limits"])
         }}

      {:ok, _payload} ->
        {:error, :invalid_runtime_state_payload}

      {:error, reason} ->
        {:error, {:invalid_runtime_state_json, reason}}
    end
  end

  defp empty_snapshot do
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
    }
  end

  defp normalize_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp normalize_value(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp normalize_value(%Date{} = value), do: Date.to_iso8601(value)
  defp normalize_value(%Time{} = value), do: Time.to_iso8601(value)
  defp normalize_value(%_{} = value), do: value |> Map.from_struct() |> normalize_value()

  defp normalize_value(value) when is_map(value) do
    Map.new(value, fn {key, nested} ->
      {to_string(key), normalize_value(nested)}
    end)
  end

  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value), do: value

  defp list_value(value) when is_list(value), do: value
  defp list_value(_value), do: []

  defp map_value(value) when is_map(value), do: value

  defp map_value(_value) do
    %{
      "input_tokens" => 0,
      "output_tokens" => 0,
      "total_tokens" => 0,
      "seconds_running" => 0
    }
  end

  defp optional_map_value(nil), do: nil
  defp optional_map_value(value) when is_map(value), do: value
  defp optional_map_value(_value), do: nil
end
