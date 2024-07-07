function google_cloud(tag, timestamp, record)
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
    new_record["timestamp"] = nil
    year, month, day, hour, minute, second, milli = record_timestamp:match("(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+).(%d+)Z")
    if not year then
      year, month, day, hour, minute, second = record_timestamp:match("(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+)Z")
    end
    timestamp = os.time({year=year, month=month, day=day, hour=hour, min=minute, sec=second})
    if milli then
      timestamp = timestamp + tonumber(milli) / 1000
    end
  end

  return 1, timestamp, new_record
end