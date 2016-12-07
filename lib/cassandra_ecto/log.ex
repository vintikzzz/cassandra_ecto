defmodule Cassandra.Ecto.Log do
  @moduledoc """
  Manages logging
  """

  def log(repo, params, entry, opts) do
    %{connection_time: query_time, decode_time: decode_time,
      pool_time: queue_time, result: result, query: query} = entry
    source = Keyword.get(opts, :source)
    repo.__log__(%Ecto.LogEntry{query_time: query_time, decode_time: decode_time,
                                queue_time: queue_time, result: result,
                                params: params, query: query,
                                ansi_color: color(query), source: source})
  end

  defp color("SELECT" <> _), do: :cyan
  defp color("INSERT" <> _), do: :green
  defp color("UPDATE" <> _), do: :yellow
  defp color("DELETE" <> _), do: :red
  defp color("BEGIN"  <> _), do: :magenta
  defp color(_), do: nil
end
