defmodule SymphonyElixir.AgentRunner do
  @moduledoc """
  Executes a single Linear issue in its workspace with Codex.
  """

  require Logger
  alias SymphonyElixir.Codex.AppServer
  alias SymphonyElixir.{Config, Linear.Issue, PromptBuilder, Tracker, Workspace}

  @type worker_host :: String.t() | nil

  @spec run(map(), pid() | nil, keyword()) :: :ok | no_return()
  def run(issue, codex_update_recipient \\ nil, opts \\ []) do
    # The orchestrator owns host retries so one worker lifetime never hops machines.
    worker_host = selected_worker_host(Keyword.get(opts, :worker_host), Config.settings!().worker.ssh_hosts)

    Logger.info("Starting agent run for #{issue_context(issue)} worker_host=#{worker_host_for_log(worker_host)}")

    case run_on_worker_host(issue, codex_update_recipient, opts, worker_host) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("Agent run failed for #{issue_context(issue)}: #{inspect(reason)}")
        raise RuntimeError, "Agent run failed for #{issue_context(issue)}: #{inspect(reason)}"
    end
  end

  defp run_on_worker_host(issue, codex_update_recipient, opts, worker_host) do
    Logger.info("Starting worker attempt for #{issue_context(issue)} worker_host=#{worker_host_for_log(worker_host)}")

    case Workspace.create_for_issue(issue, worker_host) do
      {:ok, workspace} ->
        with {:ok, codex_cwd} <- Workspace.codex_cwd(workspace, worker_host),
             {:ok, prepared_issue} <- prepare_issue_for_execution(issue, codex_cwd, worker_host, opts) do
          send_worker_runtime_info(codex_update_recipient, issue, worker_host, codex_cwd)

          try do
            with :ok <- Workspace.run_before_run_hook(codex_cwd, issue, worker_host) do
              case run_codex_turns(codex_cwd, prepared_issue, codex_update_recipient, opts, worker_host) do
                {:error, %{reason: %{type: :turn_input_required, payload: payload} = reason}} ->
                  maybe_comment_partial_response(prepared_issue, reason, opts)
                  handle_question_required(prepared_issue, payload)

                {:error, %{reason: %{type: :approval_required, payload: payload} = reason}} ->
                  maybe_comment_partial_response(prepared_issue, reason, opts)
                  handle_question_required(prepared_issue, payload)

                other ->
                  other
              end
            end
          after
            Workspace.run_after_run_hook(codex_cwd, issue, worker_host)
          end
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp codex_message_handler(recipient, issue) do
    fn message ->
      send_codex_update(recipient, issue, message)
    end
  end

  defp send_codex_update(recipient, %Issue{id: issue_id}, message)
       when is_binary(issue_id) and is_pid(recipient) do
    send(recipient, {:codex_worker_update, issue_id, message})
    :ok
  end

  defp send_codex_update(_recipient, _issue, _message), do: :ok

  defp send_worker_runtime_info(recipient, %Issue{id: issue_id}, worker_host, workspace)
       when is_binary(issue_id) and is_pid(recipient) and is_binary(workspace) do
    send(
      recipient,
      {:worker_runtime_info, issue_id,
       %{
         worker_host: worker_host,
         workspace_path: workspace
       }}
    )

    :ok
  end

  defp send_worker_runtime_info(_recipient, _issue, _worker_host, _workspace), do: :ok

  defp run_codex_turns(workspace, issue, codex_update_recipient, opts, worker_host) do
    max_turns = Keyword.get(opts, :max_turns, Config.settings!().agent.max_turns)
    issue_state_fetcher = Keyword.get(opts, :issue_state_fetcher, &Tracker.fetch_issue_states_by_ids/1)

    with {:ok, session} <- AppServer.start_session(workspace, worker_host: worker_host) do
      try do
        do_run_codex_turns(session, workspace, issue, codex_update_recipient, opts, issue_state_fetcher, 1, max_turns)
      after
        AppServer.stop_session(session)
      end
    end
  end

  defp do_run_codex_turns(app_session, workspace, issue, codex_update_recipient, opts, issue_state_fetcher, turn_number, max_turns) do
    prompt = build_turn_prompt(issue, opts, turn_number, max_turns)

    with {:ok, turn_session} <-
           AppServer.run_turn(
             app_session,
             prompt,
             issue,
             on_message: codex_message_handler(codex_update_recipient, issue)
           ) do
      Logger.info("Completed agent run for #{issue_context(issue)} session_id=#{turn_session[:session_id]} workspace=#{workspace} turn=#{turn_number}/#{max_turns}")
      maybe_comment_turn_response(issue, turn_session, turn_number, max_turns)

      case continue_with_issue?(issue, issue_state_fetcher) do
        {:continue, refreshed_issue} when turn_number < max_turns ->
          Logger.info("Continuing agent run for #{issue_context(refreshed_issue)} after normal turn completion turn=#{turn_number}/#{max_turns}")

          do_run_codex_turns(
            app_session,
            workspace,
            refreshed_issue,
            codex_update_recipient,
            opts,
            issue_state_fetcher,
            turn_number + 1,
            max_turns
          )

        {:continue, refreshed_issue} ->
          Logger.info("Reached agent.max_turns for #{issue_context(refreshed_issue)} with issue still active; returning control to orchestrator")

          :ok

        {:done, _refreshed_issue} ->
          :ok

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp build_turn_prompt(issue, opts, turn_number, max_turns) do
    steer_block = steer_comment_block(opts[:steer_comments])
    base_prompt = if turn_number == 1, do: PromptBuilder.build_prompt(issue, opts), else: continuation_prompt(turn_number, max_turns)

    case steer_block do
      nil -> base_prompt
      block -> String.trim_trailing(base_prompt) <> "\n\n" <> block
    end
  end

  defp continuation_prompt(turn_number, max_turns) do
    """
    Continuation guidance:

    - The previous Codex turn completed normally, but the Linear issue is still in an active state.
    - This is continuation turn ##{turn_number} of #{max_turns} for the current agent run.
    - Resume from the current workspace and workpad state instead of restarting from scratch.
    - The original task instructions and prior turn context are already present in this thread, so do not restate them before acting.
    - Focus on the remaining ticket work and do not end the turn while the issue stays active unless you are truly blocked.
    """
  end

  defp prepare_issue_for_execution(%Issue{} = issue, workspace, worker_host, opts) when is_binary(workspace) do
    with {:ok, prepared_issue} <- ensure_issue_in_progress(issue) do
      maybe_comment_start(prepared_issue, workspace, worker_host, opts)
      {:ok, prepared_issue}
    end
  end

  defp prepare_issue_for_execution(issue, _workspace, _worker_host, _opts), do: {:ok, issue}

  defp ensure_issue_in_progress(%Issue{id: issue_id, state: state_name} = issue)
       when is_binary(issue_id) and is_binary(state_name) do
    if normalize_issue_state(state_name) == "todo" do
      case Tracker.update_issue_state(issue_id, "In Progress") do
        :ok ->
          Logger.info("Transitioned issue to In Progress before Codex work for #{issue_context(issue)}")
          {:ok, %{issue | state: "In Progress"}}

        {:error, reason} ->
          Logger.warning("Failed to transition issue to In Progress before Codex work for #{issue_context(issue)}: #{inspect(reason)}")
          {:error, {:issue_state_transition_failed, reason}}
      end
    else
      {:ok, issue}
    end
  end

  defp ensure_issue_in_progress(issue), do: {:ok, issue}

  defp maybe_comment_start(%Issue{id: issue_id} = issue, workspace, worker_host, opts)
       when is_binary(issue_id) and is_binary(workspace) do
    if initial_attempt?(opts[:attempt]) do
      body = format_start_comment(issue, workspace, worker_host)

      case Tracker.create_comment(issue_id, body) do
        :ok ->
          Logger.info("Posted Linear activity comment for #{issue_context(issue)} workspace=#{workspace}")
          :ok

        {:error, reason} ->
          Logger.warning("Failed to post Linear activity comment for #{issue_context(issue)} workspace=#{workspace}: #{inspect(reason)}")
          :ok
      end
    else
      :ok
    end
  end

  defp maybe_comment_start(_issue, _workspace, _worker_host, _opts), do: :ok

  defp format_start_comment(_issue, workspace, worker_host) do
    """
    ## Codex 作業開始

    この issue の作業を開始しました。
    実行先: `#{worker_host_for_log(worker_host)}`
    ワークスペース: `#{workspace}`
    """
    |> String.trim()
  end

  defp maybe_comment_turn_response(
         %Issue{id: issue_id} = issue,
         %{result: result} = turn_session,
         turn_number,
         max_turns
       )
       when is_binary(issue_id) and is_integer(turn_number) and is_integer(max_turns) do
    case final_response_text(result) do
      response_text when is_binary(response_text) and response_text != "" ->
        body = format_turn_response_comment(issue, turn_session, response_text, turn_number, max_turns)

        case Tracker.create_comment(issue_id, body) do
          :ok ->
            Logger.info("Posted Linear turn response comment for #{issue_context(issue)} session_id=#{turn_session[:session_id]} turn=#{turn_number}/#{max_turns}")

            :ok

          {:error, reason} ->
            Logger.warning("Failed to post Linear turn response comment for #{issue_context(issue)} session_id=#{turn_session[:session_id]} turn=#{turn_number}/#{max_turns}: #{inspect(reason)}")

            :ok
        end

      _ ->
        :ok
    end
  end

  defp maybe_comment_turn_response(_issue, _turn_session, _turn_number, _max_turns), do: :ok

  defp maybe_comment_partial_response(
         %Issue{id: issue_id} = issue,
         %{partial_response: partial_response},
         opts
       )
       when is_binary(issue_id) and is_binary(partial_response) do
    case String.trim(partial_response) do
      "" ->
        :ok

      response_text ->
        turn_number = Keyword.get(opts, :attempt, 0) + 1
        max_turns = Keyword.get(opts, :max_turns, Config.settings!().agent.max_turns)
        body = format_turn_response_comment(issue, %{session_id: nil}, response_text, turn_number, max_turns)

        case Tracker.create_comment(issue_id, body) do
          :ok -> :ok
          {:error, _reason} -> :ok
        end
    end
  end

  defp maybe_comment_partial_response(_issue, _reason, _opts), do: :ok

  defp final_response_text(%{final_response: final_response}) when is_binary(final_response) do
    case String.trim(final_response) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp final_response_text(_result), do: nil

  defp format_turn_response_comment(_issue, turn_session, response_text, turn_number, max_turns) do
    session_suffix =
      case turn_session[:session_id] do
        session_id when is_binary(session_id) and session_id != "" ->
          "\nセッション: `#{session_id}`"

        _ ->
          ""
      end

    """
    ## Codex 応答

    ターン: #{turn_number}/#{max_turns}#{session_suffix}

    #{response_text}
    """
    |> String.trim()
  end

  defp handle_question_required(%Issue{id: issue_id} = issue, payload) when is_binary(issue_id) do
    with :ok <- Tracker.update_issue_state(issue_id, "Question") do
      body = format_question_comment(issue, payload)

      case Tracker.create_comment(issue_id, body) do
        :ok ->
          Logger.info("Moved issue to Question after input was required for #{issue_context(issue)}")
          :ok

        {:error, reason} ->
          Logger.warning("Failed to post Question comment for #{issue_context(issue)}: #{inspect(reason)}")
          :ok
      end
    else
      {:error, reason} ->
        Logger.warning("Failed to move issue to Question for #{issue_context(issue)}: #{inspect(reason)}")
        {:error, {:issue_state_transition_failed, reason}}
    end
  end

  defp handle_question_required(_issue, _payload), do: :ok

  defp format_question_comment(_issue, payload) do
    question =
      extract_first_present(payload, [
        ["params", "question"],
        [:params, :question],
        ["params", "message"],
        [:params, :message],
        ["params", "prompt"],
        [:params, :prompt],
        ["params", "request", "question"],
        [:params, :request, :question],
        ["params", "questions", 0, "question"],
        [:params, :questions, 0, :question],
        ["params", "_meta", "tool_title"],
        [:params, :_meta, :tool_title]
      ]) || "追加の確認が必要です。"

    """
    ## Codex 質問

    作業を進める前に確認したいことがあります。

    #{question}

    回答後に issue を `Todo` または `In Progress` に戻すと再開します。
    """
    |> String.trim()
  end

  defp steer_comment_block(nil), do: nil

  defp steer_comment_block(comments) when is_list(comments) do
    lines =
      comments
      |> Enum.map(&format_steer_comment/1)
      |> Enum.reject(&is_nil/1)

    case lines do
      [] ->
        nil

      entries ->
        """
        Operator steer from new Linear comments:

        #{Enum.join(entries, "\n\n")}

        Treat the steer comments above as the newest operator instructions for this issue.
        """
        |> String.trim()
    end
  end

  defp steer_comment_block(_comments), do: nil

  defp format_steer_comment(%{body: body} = comment) when is_binary(body) do
    author =
      case comment[:user_name] do
        name when is_binary(name) and name != "" -> name
        _ -> "unknown"
      end

    timestamp =
      case comment[:created_at] do
        %DateTime{} = created_at -> " at #{DateTime.to_iso8601(created_at)}"
        _ -> ""
      end

    "- #{author}#{timestamp}\n#{String.trim(body)}"
  end

  defp format_steer_comment(body) when is_binary(body) do
    "- #{String.trim(body)}"
  end

  defp format_steer_comment(_comment), do: nil

  defp initial_attempt?(attempt) when is_integer(attempt), do: attempt <= 0
  defp initial_attempt?(nil), do: true
  defp initial_attempt?(_attempt), do: false

  defp extract_first_present(payload, paths) when is_map(payload) and is_list(paths) do
    Enum.find_value(paths, fn path -> map_path(payload, path) end)
  end

  defp extract_first_present(_payload, _paths), do: nil

  defp map_path(data, [key | rest]) when is_map(data) do
    case Map.fetch(data, key) do
      {:ok, value} when rest == [] -> value
      {:ok, value} -> map_path(value, rest)
      :error -> nil
    end
  end

  defp map_path(data, [index | rest]) when is_list(data) and is_integer(index) do
    case Enum.at(data, index) do
      nil -> nil
      value when rest == [] -> value
      value -> map_path(value, rest)
    end
  end

  defp map_path(_data, _path), do: nil

  defp continue_with_issue?(%Issue{id: issue_id} = issue, issue_state_fetcher) when is_binary(issue_id) do
    case issue_state_fetcher.([issue_id]) do
      {:ok, [%Issue{} = refreshed_issue | _]} ->
        if active_issue_state?(refreshed_issue.state) do
          {:continue, refreshed_issue}
        else
          {:done, refreshed_issue}
        end

      {:ok, []} ->
        {:done, issue}

      {:error, reason} ->
        {:error, {:issue_state_refresh_failed, reason}}
    end
  end

  defp continue_with_issue?(issue, _issue_state_fetcher), do: {:done, issue}

  defp active_issue_state?(state_name) when is_binary(state_name) do
    normalized_state = normalize_issue_state(state_name)

    Config.settings!().tracker.active_states
    |> Enum.any?(fn active_state -> normalize_issue_state(active_state) == normalized_state end)
  end

  defp active_issue_state?(_state_name), do: false

  defp selected_worker_host(nil, []), do: nil

  defp selected_worker_host(preferred_host, configured_hosts) when is_list(configured_hosts) do
    hosts =
      configured_hosts
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.uniq()

    case preferred_host do
      host when is_binary(host) and host != "" -> host
      _ when hosts == [] -> nil
      _ -> List.first(hosts)
    end
  end

  defp worker_host_for_log(nil), do: "local"
  defp worker_host_for_log(worker_host), do: worker_host

  defp normalize_issue_state(state_name) when is_binary(state_name) do
    state_name
    |> String.trim()
    |> String.downcase()
  end

  defp issue_context(%Issue{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end
end
