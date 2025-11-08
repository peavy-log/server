function parse_time_string(time_string)
  year, month, day, hour, minute, second, milli = time_string:match("(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+).(%d+)Z")
  if not year then
    year, month, day, hour, minute, second = time_string:match("(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+)Z")
    milli = nil
  end
  timestamp = os.time({year=year, month=month, day=day, hour=hour, min=minute, sec=second})

  if milli then
    milli_length = string.len(milli)

    seconds = timestamp
    nanos = tonumber(milli) * (10^(9 - milli_length))

    timestamp = timestamp + (tonumber(milli) / (10^milli_length))
    return timestamp, {seconds = seconds, nanos = nanos}
  else
    return timestamp, {seconds = timestamp, nanos = 0}
  end
end

function process_record(tag, timestamp, record)
  if record["__peavy_type"] == "event" then
    return clickhouse(tag, timestamp, record)
  else
    record["__peavy_type"] = nil
    return google_cloud(tag, timestamp, record)
  end
end

function google_cloud(tag, timestamp, record)
  -- Handle re-emits
  if record["logging.googleapis.com/logName"] then
    return 0, timestamp, record
  end

  new_record = record

  new_record["logging.googleapis.com/logName"] = "peavy"
  new_record["severity"] = new_record["severity"] or "info"

  peavy_labels = new_record["peavy/labels"]
  if peavy_labels then
    new_record["peavy/labels"] = nil
    labels = {}
    for k, v in pairs(peavy_labels) do
      labels["peavy/" .. k] = tostring(v)
    end
    labels["peavy/log"] = "true"
    new_record["logging.googleapis.com/labels"] = labels
  end

  custom_labels = new_record["labels"]
  if custom_labels then
    for k, v in pairs(custom_labels) do
      new_record["logging.googleapis.com/labels"][k] = tostring(v)
    end
    new_record["labels"] = nil
  end

  trace = new_record["peavy/traceId"]
  if trace then
    new_record["logging.googleapis.com/trace"] = tostring(trace)
    new_record["peavy/traceId"] = nil
  end

  record_timestamp = new_record["timestamp"]
  if record_timestamp then
    timestamp, record_timestamp = parse_time_string(record_timestamp)
    new_record["timestamp"] = record_timestamp
  else
    new_record["timestamp"] = {
      seconds = timestamp,
      nanos = 0
    }
  end

  return 1, timestamp, new_record
end


FIELD_LABELS = {
  ["platform"] = "platform",
  ["app-id"] = "app_id",
  ["app-version-code"] = "app_version_code",
  ["session-id"] = "session_id",
}

PII_LABELS = {
  ["user-id"] = true,
  ["userId"] = true,
  ["user_id"] = true,
  ["user"] = true,
  ["id"] = true,
  ["uid"] = true,
  ["ident"] = true,
  ["user-email"] = true,
  ["email"] = true,
  ["number"] = true,
}

function clickhouse(tag, timestamp, record)
  new_record = {}
  if record["timestamp"] then
    timestamp, record_timestamp = parse_time_string(record["timestamp"])
  end

  peavy_labels = record["peavy/labels"]
  if peavy_labels then
    labels = {} -- stored, opaque, labels
    for k, v in pairs(peavy_labels) do
      if v == nil then
        v = ""
      end
      local field_key = FIELD_LABELS[k]
      if field_key then
        new_record[field_key] = v
      elseif PII_LABELS[k] then
        -- skip PII labels
      else
        labels[k] = tostring(v)
      end
    end
    new_record["labels"] = labels
  end

  new_record["type"] = tostring(record["type"] or "action")
  new_record["category"] = tostring(record["category"] or "")
  new_record["name"] = tostring(record["name"] or "")
  new_record["ident"] = tostring(record["ident"] or "")
  new_record["duration"] = record["duration"] or 0
  new_record["result"] = tostring(record["result"] or "success")

  return 1, timestamp, new_record
end