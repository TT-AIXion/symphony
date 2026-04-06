defmodule SymphonyElixir.Codex.DynamicTool do
  @moduledoc """
  Executes client-side tool calls requested by Codex app-server turns.
  """

  alias SymphonyElixir.Linear.Client

  @linear_graphql_tool "linear_graphql"
  @linear_graphql_description """
  Execute a raw GraphQL query or mutation against Linear using Symphony's configured auth.
  """
  @linear_graphql_input_schema %{
    "type" => "object",
    "additionalProperties" => false,
    "required" => ["query"],
    "properties" => %{
      "query" => %{
        "type" => "string",
        "description" => "GraphQL query or mutation document to execute against Linear."
      },
      "variables" => %{
        "type" => ["object", "null"],
        "description" => "Optional GraphQL variables object.",
        "additionalProperties" => true
      }
    }
  }

  @spec execute(String.t() | nil, term(), keyword()) :: map()
  def execute(tool, arguments, opts \\ []) do
    case tool do
      @linear_graphql_tool ->
        execute_linear_graphql(arguments, opts)

      other ->
        failure_response(%{
          "error" => %{
            "message" => "Unsupported dynamic tool: #{inspect(other)}.",
            "supportedTools" => supported_tool_names()
          }
        })
    end
  end

  @spec tool_specs() :: [map()]
  def tool_specs do
    [
      %{
        "name" => @linear_graphql_tool,
        "description" => @linear_graphql_description,
        "inputSchema" => @linear_graphql_input_schema
      }
    ]
  end

  defp execute_linear_graphql(arguments, opts) do
    linear_client = Keyword.get(opts, :linear_client, &Client.graphql/3)

    with {:ok, query, variables} <- normalize_linear_graphql_arguments(arguments),
         {:ok, response} <- linear_client.(query, variables, []) do
      graphql_response(response)
    else
      {:error, reason} ->
        failure_response(tool_error_payload(reason))
    end
  end

  defp normalize_linear_graphql_arguments(arguments) when is_binary(arguments) do
    case String.trim(arguments) do
      "" -> {:error, :missing_query}
      query -> validate_query_document(query, %{})
    end
  end

  defp normalize_linear_graphql_arguments(arguments) when is_map(arguments) do
    case normalize_query(arguments) do
      {:ok, query} ->
        case normalize_variables(arguments) do
          {:ok, variables} ->
            validate_query_document(query, variables)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_linear_graphql_arguments(_arguments), do: {:error, :invalid_arguments}

  defp normalize_query(arguments) do
    case Map.get(arguments, "query") || Map.get(arguments, :query) do
      query when is_binary(query) ->
        case String.trim(query) do
          "" -> {:error, :missing_query}
          trimmed -> {:ok, trimmed}
        end

      _ ->
        {:error, :missing_query}
    end
  end

  defp normalize_variables(arguments) do
    case Map.get(arguments, "variables") || Map.get(arguments, :variables) || %{} do
      variables when is_map(variables) -> {:ok, variables}
      _ -> {:error, :invalid_variables}
    end
  end

  defp validate_query_document(query, variables) when is_binary(query) and is_map(variables) do
    case graphql_operation_count(query) do
      1 -> {:ok, query, variables}
      _ -> {:error, :invalid_query_document}
    end
  end

  defp graphql_response(response) do
    success =
      case response do
        %{"errors" => errors} when is_list(errors) and errors != [] -> false
        %{errors: errors} when is_list(errors) and errors != [] -> false
        _ -> true
      end

    dynamic_tool_response(success, encode_payload(response))
  end

  defp failure_response(payload) do
    dynamic_tool_response(false, encode_payload(payload))
  end

  defp dynamic_tool_response(success, output) when is_boolean(success) and is_binary(output) do
    %{
      "success" => success,
      "output" => output,
      "contentItems" => [
        %{
          "type" => "inputText",
          "text" => output
        }
      ]
    }
  end

  defp encode_payload(payload) when is_map(payload) or is_list(payload) do
    Jason.encode!(payload, pretty: true)
  end

  defp encode_payload(payload), do: inspect(payload)

  defp tool_error_payload(:missing_query) do
    %{
      "error" => %{
        "message" => "`linear_graphql` requires a non-empty `query` string."
      }
    }
  end

  defp tool_error_payload(:invalid_arguments) do
    %{
      "error" => %{
        "message" => "`linear_graphql` expects either a GraphQL query string or an object with `query` and optional `variables`."
      }
    }
  end

  defp tool_error_payload(:invalid_variables) do
    %{
      "error" => %{
        "message" => "`linear_graphql.variables` must be a JSON object when provided."
      }
    }
  end

  defp tool_error_payload(:invalid_query_document) do
    %{
      "error" => %{
        "message" => "`linear_graphql.query` must contain exactly one GraphQL operation."
      }
    }
  end

  defp tool_error_payload(:missing_linear_api_token) do
    %{
      "error" => %{
        "message" => "Symphony is missing Linear auth. Set `linear.api_key` in `WORKFLOW.md` or export `LINEAR_API_KEY`."
      }
    }
  end

  defp tool_error_payload({:linear_api_status, status}) do
    %{
      "error" => %{
        "message" => "Linear GraphQL request failed with HTTP #{status}.",
        "status" => status
      }
    }
  end

  defp tool_error_payload({:linear_api_request, reason}) do
    %{
      "error" => %{
        "message" => "Linear GraphQL request failed before receiving a successful response.",
        "reason" => inspect(reason)
      }
    }
  end

  defp tool_error_payload(reason) do
    %{
      "error" => %{
        "message" => "Linear GraphQL tool execution failed.",
        "reason" => inspect(reason)
      }
    }
  end

  defp supported_tool_names do
    Enum.map(tool_specs(), & &1["name"])
  end

  defp graphql_operation_count(query) when is_binary(query) do
    query
    |> String.to_charlist()
    |> count_graphql_operations(0, :normal, 0, false)
  end

  defp count_graphql_operations([], _depth, _mode, count, _awaiting_body), do: count

  defp count_graphql_operations([?# | rest], depth, :normal, count, awaiting_body) do
    count_graphql_operations(rest, depth, :comment, count, awaiting_body)
  end

  defp count_graphql_operations([?\n | rest], depth, :comment, count, awaiting_body) do
    count_graphql_operations(rest, depth, :normal, count, awaiting_body)
  end

  defp count_graphql_operations([_char | rest], depth, :comment, count, awaiting_body) do
    count_graphql_operations(rest, depth, :comment, count, awaiting_body)
  end

  defp count_graphql_operations([?", ?", ?" | rest], depth, :normal, count, awaiting_body) do
    count_graphql_operations(rest, depth, :block_string, count, awaiting_body)
  end

  defp count_graphql_operations([?", ?", ?" | rest], depth, :block_string, count, awaiting_body) do
    count_graphql_operations(rest, depth, :normal, count, awaiting_body)
  end

  defp count_graphql_operations([?", ?", ?" | rest], depth, :string, count, awaiting_body) do
    count_graphql_operations(rest, depth, :string, count, awaiting_body)
  end

  defp count_graphql_operations([?" | rest], depth, :normal, count, awaiting_body) do
    count_graphql_operations(rest, depth, :string, count, awaiting_body)
  end

  defp count_graphql_operations([?\\, _escaped | rest], depth, :string, count, awaiting_body) do
    count_graphql_operations(rest, depth, :string, count, awaiting_body)
  end

  defp count_graphql_operations([?" | rest], depth, :string, count, awaiting_body) do
    count_graphql_operations(rest, depth, :normal, count, awaiting_body)
  end

  defp count_graphql_operations([_char | rest], depth, :string, count, awaiting_body) do
    count_graphql_operations(rest, depth, :string, count, awaiting_body)
  end

  defp count_graphql_operations([_char | rest], depth, :block_string, count, awaiting_body) do
    count_graphql_operations(rest, depth, :block_string, count, awaiting_body)
  end

  defp count_graphql_operations([char | rest], depth, :normal, count, awaiting_body)
       when char in [?\s, ?\n, ?\r, ?\t, ?,] do
    count_graphql_operations(rest, depth, :normal, count, awaiting_body)
  end

  defp count_graphql_operations([?{ | rest], 0, :normal, count, true) do
    count_graphql_operations(rest, 1, :normal, count, false)
  end

  defp count_graphql_operations([?{ | rest], 0, :normal, count, false) do
    count_graphql_operations(rest, 1, :normal, count + 1, false)
  end

  defp count_graphql_operations([?{ | rest], depth, :normal, count, awaiting_body) do
    count_graphql_operations(rest, depth + 1, :normal, count, awaiting_body)
  end

  defp count_graphql_operations([?} | rest], depth, :normal, count, awaiting_body) do
    count_graphql_operations(rest, max(depth - 1, 0), :normal, count, awaiting_body)
  end

  defp count_graphql_operations([char | rest], 0, :normal, count, _awaiting_body)
       when (char >= ?A and char <= ?Z) or (char >= ?a and char <= ?z) or char == ?_ do
    {token, remainder} = take_identifier(rest, [char])

    case token do
      "query" -> count_graphql_operations(remainder, 0, :normal, count + 1, true)
      "mutation" -> count_graphql_operations(remainder, 0, :normal, count + 1, true)
      "subscription" -> count_graphql_operations(remainder, 0, :normal, count + 1, true)
      _ -> count_graphql_operations(remainder, 0, :normal, count, true)
    end
  end

  defp count_graphql_operations([_char | rest], depth, :normal, count, awaiting_body) do
    count_graphql_operations(rest, depth, :normal, count, awaiting_body)
  end

  defp take_identifier([char | rest], acc)
       when (char >= ?A and char <= ?Z) or (char >= ?a and char <= ?z) or
              (char >= ?0 and char <= ?9) or char == ?_ do
    take_identifier(rest, [char | acc])
  end

  defp take_identifier(rest, acc) do
    {acc |> Enum.reverse() |> to_string(), rest}
  end
end
