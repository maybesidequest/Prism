defmodule Prism.DiscordWorker do
  require Logger
  require OpenTelemetry.Tracer

  alias Prism.Helpers
  alias Prism.DiscordWorker.{HTTP, Retry, Callbacks, DeadMessage}

  @doc """
  Sends the webhook content to a guild's discord webhook URL with retry logic.
  Returns `{:ok, message_id}` on success or `{:error, reason}` on failure.
  """
  def process_target(
        action,
        target,
        content \\ %{},
        batch_id \\ nil,
        polled_at \\ nil,
        enqueued_at \\ nil,
        parent_message_id \\ nil,
        opts \\ []
      )

  def process_target(
        action,
        %{"webhook_id" => webhook_id, "webhook_token" => webhook_token} = target,
        content,
        batch_id,
        polled_at,
        enqueued_at,
        parent_message_id,
        opts
      ) do
    preflight = Keyword.get(opts, :preflight)
    skip_checkpoint_write = Keyword.get(opts, :skip_checkpoint_write, false)
    parent_msg_id = if action == "execute", do: parent_message_id, else: nil

    if is_binary(webhook_id) and is_binary(webhook_token) do
      base_url = "#{Prism.Config.discord_base_url()}/api/webhooks/#{webhook_id}/#{webhook_token}"
      thread_id = Map.get(target, "thread_id")
      message_id = Map.get(target, "message_id")

      if dead_cache_hit?(action, webhook_id, message_id) do
        Logger.debug(
          "Skipping #{action} for webhook_id=#{webhook_id} message_id=#{message_id} — known dead in cache"
        )

        if action == "delete", do: {:ok, nil}, else: {:error, :message_not_found}
      else
        headers = [{"Content-Type", "application/json"}]
        pre_encoded_body = Keyword.get(opts, :pre_encoded_body)

        body =
          if action == "delete" do
            nil
          else
            overrides = Map.get(target, "overrides")

            if is_map(content) and action in ["execute", "edit"] and is_map(overrides) and
                 map_size(overrides) > 0 do
              merged = Helpers.merge_overrides(content, target, action)
              Jason.encode_to_iodata!(merged)
            else
              pre_encoded_body || Jason.encode_to_iodata!(content)
            end
          end

        polarizer_action_id = Map.get(target, "polarizer_action_id")

        checkpoint_key =
          Helpers.checkpoint_key(action, batch_id, webhook_id, polarizer_action_id)

        method_str = Helpers.action_to_method_string(action)

        cached_result =
          cond do
            preflight && preflight.checkpoint == :not_found ->
              nil

            preflight && preflight.checkpoint == {:done} ->
              {:ok, nil}

            preflight && match?({:ok, _}, preflight.checkpoint) ->
              {:ok, elem(preflight.checkpoint, 1)}

            batch_id ->
              case Helpers.redix_command(["GET", checkpoint_key]) do
                {:ok, "done"} -> {:ok, nil}
                {:ok, msg_id} when is_binary(msg_id) -> {:ok, msg_id}
                _ -> nil
              end

            true ->
              nil
          end

        OpenTelemetry.Tracer.with_span "prism.worker.process_target" do
          OpenTelemetry.Tracer.set_attributes([
            {:action, action},
            {:webhook_id, webhook_id},
            {:batch_id, batch_id}
          ])

          if cached_result do
            cached_result
          else
            if Prism.Config.backpressure_enabled?() and Prism.RateLimit.unhealthy?() do
              delay_ms = Prism.RateLimit.backoff_ms()

              case HTTP.build_request(action, base_url, message_id, thread_id) do
                {:ok, method, url} ->
                  Retry.spawn_retry(
                    action,
                    target,
                    method,
                    url,
                    headers,
                    body,
                    webhook_id,
                    message_id,
                    batch_id,
                    delay_ms,
                    1,
                    parent_msg_id,
                    :rate_limited
                  )

                  {:error, {:rate_limited, delay_ms}}

                {:error, reason} ->
                  {:error, reason}
              end
            else
              defer_threshold = Prism.Config.rate_limit_defer_threshold_ms()

              {should_defer, should_sleep, rate_limit_delay_ms} =
                if preflight do
                  defer_or_sleep(preflight.rate_limit, defer_threshold)
                else
                  defer_or_sleep(Prism.RateLimit.check(webhook_id, method_str), defer_threshold)
                end

              if should_defer do
                Logger.debug(
                  "Pre-flight rate limit check triggered for webhook_id=#{webhook_id} with long TTL=#{rate_limit_delay_ms}ms. Rescheduling immediately."
                )

                case HTTP.build_request(action, base_url, message_id, thread_id) do
                  {:ok, method, url} ->
                    Retry.spawn_retry(
                      action,
                      target,
                      method,
                      url,
                      headers,
                      body,
                      webhook_id,
                      message_id,
                      batch_id,
                      rate_limit_delay_ms,
                      1,
                      parent_msg_id,
                      :rate_limited
                    )

                    {:error, {:rate_limited, rate_limit_delay_ms}}

                  {:error, reason} ->
                    {:error, reason}
                end
              else
                if should_sleep do
                  Logger.debug(
                    "Pre-flight rate limit check triggered for webhook_id=#{webhook_id}. Sleeping for #{rate_limit_delay_ms}ms."
                  )

                  Process.sleep(rate_limit_delay_ms)
                end

                case HTTP.build_request(action, base_url, message_id, thread_id) do
                  {:ok, method, url} ->
                    req_start = System.monotonic_time(:millisecond)
                    req_start_wall = :os.system_time(:millisecond)

                    result =
                      HTTP.do_http_request(
                        method,
                        method_str,
                        url,
                        headers,
                        body,
                        webhook_id,
                        message_id
                      )

                    req_end = System.monotonic_time(:millisecond)
                    req_end_wall = :os.system_time(:millisecond)

                    timing =
                      Prism.DeliveryTiming.record_completed(
                        enqueued_at || polled_at || req_start_wall,
                        polled_at || req_start_wall,
                        req_start_wall,
                        req_end_wall,
                        req_end - req_start,
                        %{
                          action: action,
                          batch_id: batch_id,
                          webhook_id: webhook_id,
                          success: match?({:ok, _}, result)
                        }
                      )

                    if Code.ensure_loaded?(Mix) and Mix.env() == :dev do
                      Logger.info(
                        "[Timing] Webhook #{webhook_id} (batch #{batch_id || "N/A"}) - Queue: #{timing.queue_ms}ms | Prep: #{timing.preparation_ms}ms | Discord HTTP: #{timing.http_ms}ms | Total End-to-End: #{timing.origin_to_delivery_ms}ms"
                      )
                    end

                    write_checkpoint(
                      skip_checkpoint_write,
                      batch_id,
                      action,
                      webhook_id,
                      polarizer_action_id,
                      result
                    )

                    case result do
                      {:error, {:rate_limited, delay_ms}} ->
                        OpenTelemetry.Tracer.set_attribute(:error_type, "rate_limited")

                        Retry.spawn_retry(
                          action,
                          target,
                          method,
                          url,
                          headers,
                          body,
                          webhook_id,
                          message_id,
                          batch_id,
                          delay_ms,
                          1,
                          parent_msg_id,
                          :rate_limited
                        )

                        {:error, {:rate_limited, delay_ms}}

                      {:error, {:congestion_backoff, delay_ms}} ->
                        OpenTelemetry.Tracer.set_attribute(:error_type, "congestion_backoff")

                        Retry.spawn_retry(
                          action,
                          target,
                          method,
                          url,
                          headers,
                          body,
                          webhook_id,
                          message_id,
                          batch_id,
                          delay_ms,
                          1,
                          parent_msg_id,
                          :congestion_backoff
                        )

                        {:error, {:congestion_backoff, delay_ms}}

                      {:error, {:server_error, _}} ->
                        OpenTelemetry.Tracer.set_attribute(:error_type, "server_error")

                        Retry.spawn_retry(
                          action,
                          target,
                          method,
                          url,
                          headers,
                          body,
                          webhook_id,
                          message_id,
                          batch_id,
                          Prism.Config.server_error_base_delay_ms(),
                          1,
                          parent_msg_id,
                          :server_error
                        )

                        {:ok, nil}

                      {:error, :network_error} ->
                        OpenTelemetry.Tracer.set_attribute(:error_type, "network_error")

                        Retry.spawn_retry(
                          action,
                          target,
                          method,
                          url,
                          headers,
                          body,
                          webhook_id,
                          message_id,
                          batch_id,
                          Prism.Config.network_error_base_delay_ms(),
                          1,
                          parent_msg_id,
                          :network_error
                        )

                        {:ok, nil}

                      {:error, :message_not_found_transient} ->
                        OpenTelemetry.Tracer.set_attribute(
                          :error_type,
                          "message_not_found_transient"
                        )

                        Retry.spawn_retry(
                          action,
                          target,
                          method,
                          url,
                          headers,
                          body,
                          webhook_id,
                          message_id,
                          batch_id,
                          Prism.Config.network_error_base_delay_ms(),
                          1,
                          parent_msg_id,
                          :message_not_found_transient
                        )

                        {:ok, nil}

                      {:error, :permanent} ->
                        OpenTelemetry.Tracer.set_attribute(:error_type, "permanent")
                        {:error, :permanent}

                      other ->
                        other
                    end

                  {:error, reason} ->
                    OpenTelemetry.Tracer.set_attribute(:error_type, inspect(reason))
                    {:error, reason}
                end
              end
            end
          end
        end
      end
    else
      Logger.warning("Invalid webhook data. Skipping.")
      {:error, :invalid_webhook}
    end
  end

  def process_target(
        _action,
        target,
        _content,
        _batch_id,
        _polled_at,
        _enqueued_at,
        _parent_message_id,
        _opts
      ) do
    Logger.warning("Missing webhook data in target: #{inspect(target)}. Skipping.")
    {:error, :missing_webhook}
  end

  def process_retry(payload, _polled_at, _enqueued_at, opts \\ []) do
    skip_checkpoint_write = Keyword.get(opts, :skip_checkpoint_write, false)

    source_message_id =
      payload["source_message_id"] || payload["parent_msg_id"] || payload["batch_id"]

    # Deletes are exempt from the cancel gate: the flag marks a source message as
    # gone, which is precisely when its copies still need removing.
    if payload["action"] != "delete" && source_message_id &&
         Prism.CancelChecker.cancelled?(source_message_id) do
      Logger.info("RetryBroadway: skipping cancelled retry for #{source_message_id}")
    else
      action = payload["action"]
      target = payload["target"]
      method = Helpers.safe_method_atom(payload["method"])
      method_str = to_string(method)
      url = payload["url"]

      headers =
        case payload["headers"] do
          nil -> []
          map when is_map(map) -> Enum.to_list(map)
          list when is_list(list) -> list
        end

      body = payload["body"]
      webhook_id = payload["webhook_id"]
      message_id = payload["message_id"]
      batch_id = payload["batch_id"]
      attempt = payload["attempt"]
      parent_msg_id = payload["parent_msg_id"]
      reason = payload["reason"]

      reason_str = if reason, do: " (Reason: #{reason})", else: ""

      Logger.debug(
        "Retrying webhook_id=#{webhook_id} (Attempt #{attempt})#{reason_str} in Broadway pipeline..."
      )

      if Prism.Config.backpressure_enabled?() and Prism.RateLimit.unhealthy?() do
        delay_ms = Prism.RateLimit.backoff_ms()

        Retry.spawn_retry(
          action,
          target,
          method,
          url,
          headers,
          body,
          webhook_id,
          message_id,
          batch_id,
          delay_ms,
          attempt,
          parent_msg_id,
          :rate_limited
        )
      else
        if dead_cache_hit?(action, webhook_id, message_id) do
          Logger.debug(
            "Retry skipping #{action} for webhook_id=#{webhook_id} message_id=#{message_id} — known dead in cache"
          )

          if action == "delete" do
            Callbacks.publish_partial(action, target, batch_id, parent_msg_id, nil, nil)
          else
            Callbacks.publish_partial(
              action,
              target,
              batch_id,
              parent_msg_id,
              nil,
              :message_not_found
            )
          end
        else
          defer_threshold = Prism.Config.rate_limit_defer_threshold_ms()

          {should_defer, should_sleep, rate_limit_delay_ms} =
            defer_or_sleep(Prism.RateLimit.check(webhook_id, method_str), defer_threshold)

          if should_defer do
            Logger.debug(
              "Retry pre-flight rate limit triggered for webhook_id=#{webhook_id} with long TTL=#{rate_limit_delay_ms}ms. Rescheduling immediately."
            )

            Retry.spawn_retry(
              action,
              target,
              method,
              url,
              headers,
              body,
              webhook_id,
              message_id,
              batch_id,
              rate_limit_delay_ms,
              attempt,
              parent_msg_id,
              :rate_limited
            )
          else
            if should_sleep do
              Logger.debug(
                "Retry pre-flight rate limit triggered for webhook_id=#{webhook_id}. Sleeping for #{rate_limit_delay_ms}ms."
              )

              Process.sleep(rate_limit_delay_ms)
            end

            result =
              HTTP.do_http_request(method, method_str, url, headers, body, webhook_id, message_id)

            case result do
              {:error, {:rate_limited, delay_ms}} ->
                Retry.spawn_retry(
                  action,
                  target,
                  method,
                  url,
                  headers,
                  body,
                  webhook_id,
                  message_id,
                  batch_id,
                  delay_ms,
                  attempt + 1,
                  parent_msg_id,
                  :rate_limited
                )

              {:error, {:congestion_backoff, delay_ms}} ->
                Retry.spawn_retry(
                  action,
                  target,
                  method,
                  url,
                  headers,
                  body,
                  webhook_id,
                  message_id,
                  batch_id,
                  delay_ms,
                  attempt + 1,
                  parent_msg_id,
                  :congestion_backoff
                )

              {:error, {:server_error, _}} ->
                max_retries = Prism.Config.server_error_max_retries()

                if attempt >= max_retries do
                  Callbacks.publish_partial(
                    action,
                    target,
                    batch_id,
                    parent_msg_id,
                    nil,
                    :server_error
                  )
                else
                  backoff_ms = Prism.Config.server_error_base_delay_ms() * attempt
                  jitter_ms = :rand.uniform(1000)
                  delay_ms = backoff_ms + jitter_ms

                  Retry.spawn_retry(
                    action,
                    target,
                    method,
                    url,
                    headers,
                    body,
                    webhook_id,
                    message_id,
                    batch_id,
                    delay_ms,
                    attempt + 1,
                    parent_msg_id,
                    :server_error
                  )
                end

              {:error, :network_error} ->
                max_retries = Prism.Config.network_error_max_retries()

                if attempt >= max_retries do
                  Callbacks.publish_partial(
                    action,
                    target,
                    batch_id,
                    parent_msg_id,
                    nil,
                    :network_error
                  )
                else
                  backoff_ms = Prism.Config.network_error_base_delay_ms() * attempt
                  jitter_ms = :rand.uniform(500)
                  delay_ms = backoff_ms + jitter_ms

                  Retry.spawn_retry(
                    action,
                    target,
                    method,
                    url,
                    headers,
                    body,
                    webhook_id,
                    message_id,
                    batch_id,
                    delay_ms,
                    attempt + 1,
                    parent_msg_id,
                    :network_error
                  )
                end

              {:error, :message_not_found_transient} ->
                max_retries = Prism.Config.message_not_found_max_retries()

                if attempt >= max_retries do
                  if method == :delete do
                    Logger.info(
                      "Webhook_id=#{webhook_id} still 10008 on attempt #{attempt} for delete, assuming deleted."
                    )

                    Callbacks.publish_partial(action, target, batch_id, parent_msg_id, nil, nil)
                  else
                    Callbacks.publish_partial(
                      action,
                      target,
                      batch_id,
                      parent_msg_id,
                      nil,
                      :message_not_found
                    )
                  end
                else
                  backoff_ms = Prism.Config.network_error_base_delay_ms() * attempt
                  jitter_ms = :rand.uniform(500)
                  delay_ms = backoff_ms + jitter_ms

                  Retry.spawn_retry(
                    action,
                    target,
                    method,
                    url,
                    headers,
                    body,
                    webhook_id,
                    message_id,
                    batch_id,
                    delay_ms,
                    attempt + 1,
                    parent_msg_id,
                    :message_not_found_transient
                  )
                end

              {:error, :permanent} ->
                Logger.warning("Permanent error for webhook_id=#{webhook_id}, not retrying.")

                Callbacks.publish_partial(
                  action,
                  target,
                  batch_id,
                  parent_msg_id,
                  nil,
                  :permanent
                )

              {:error, :invalid_webhook} ->
                Logger.warning(
                  "Invalid webhook error for webhook_id=#{webhook_id}, not retrying."
                )

                Callbacks.publish_partial(
                  action,
                  target,
                  batch_id,
                  parent_msg_id,
                  nil,
                  :invalid_webhook
                )

              {:error, :message_not_found} ->
                Logger.info("Message not found error for webhook_id=#{webhook_id}, not retrying.")

                Callbacks.publish_partial(
                  action,
                  target,
                  batch_id,
                  parent_msg_id,
                  nil,
                  :message_not_found
                )

              {:error, :empty_payload} ->
                Logger.warning("Empty payload error for webhook_id=#{webhook_id}, not retrying.")

                Callbacks.publish_partial(
                  action,
                  target,
                  batch_id,
                  parent_msg_id,
                  nil,
                  :empty_payload
                )

              {:ok, msg_id} ->
                Logger.info(
                  "Successfully delivered to webhook_id=#{webhook_id} on Attempt #{attempt}!"
                )

                Prism.RateLimit.record_success()

                write_checkpoint(
                  skip_checkpoint_write,
                  batch_id,
                  action,
                  webhook_id,
                  Map.get(target, "polarizer_action_id"),
                  {:ok, msg_id}
                )

                Callbacks.publish_partial(action, target, batch_id, parent_msg_id, msg_id, nil)

              other ->
                Logger.error(
                  "Unexpected retry result for webhook_id=#{webhook_id}: #{inspect(other)}"
                )

                Callbacks.publish_partial(action, target, batch_id, parent_msg_id, nil, other)
            end
          end
        end
      end
    end
  end

  defp write_checkpoint(skip?, batch_id, action, webhook_id, polarizer_action_id, result) do
    if batch_id && not skip? do
      checkpoint_key =
        Helpers.checkpoint_key(action, batch_id, webhook_id, polarizer_action_id)

      checkpoint_ttl = to_string(Prism.Config.checkpoint_ttl_seconds())

      case result do
        {:ok, msg_id} when is_binary(msg_id) ->
          Helpers.redix_command(["SETEX", checkpoint_key, checkpoint_ttl, msg_id])

        {:ok, _} ->
          Helpers.redix_command(["SETEX", checkpoint_key, checkpoint_ttl, "done"])

        _ ->
          :ok
      end
    else
      :ok
    end
  end

  defp dead_cache_hit?(action, webhook_id, message_id) do
    action in ["edit", "delete"] and is_binary(message_id) and
      DeadMessage.dead_message_cached?(webhook_id, message_id)
  end

  defp defer_or_sleep(rate_limit_result, defer_threshold) do
    case rate_limit_result do
      {:ok, _remaining} ->
        {false, false, 0}

      {:blocked, ttl} ->
        if ttl > defer_threshold,
          do: {true, false, ttl},
          else: {false, true, ttl}
    end
  end
end
