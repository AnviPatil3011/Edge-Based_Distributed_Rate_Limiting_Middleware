# rate_limiter/redis_scripts.py

# Central repository of Lua scripts used by the rate limiter and sync worker.
# Each script is stored as a string and registered with RedisService.

# Token Bucket script (keys: 1) rl:tb:{user_id}:{policy_id}, 2) metrics counters key)
# ARGV: 1) limit, 2) refill_rate (tokens per second), 3) bucket_capacity, 4) current timestamp (ms)
TOKEN_BUCKET_SCRIPT = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

-- Get existing state
local data = redis.call('HMGET', key, 'tokens', 'last_ts')
local tokens = tonumber(data[1])
local last_ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  last_ts = now
else
  local elapsed = (now - last_ts) / 1000.0
  local refill = elapsed * refill_rate
  tokens = math.min(capacity, tokens + refill)
  last_ts = now
end

local allowed = 0
local remaining = 0
local retry_after = nil
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
  remaining = math.floor(tokens)
else
  allowed = 0
  remaining = 0
  -- calculate time until next token
  local needed = 1 - tokens
  retry_after = math.ceil((needed / refill_rate) * 1000)
end

-- Store updated state
redis.call('HMSET', key, 'tokens', tokens, 'last_ts', last_ts)
-- Set TTL to 2 * window seconds (provided via limit as window)
redis.call('EXPIRE', key, math.ceil(limit * 2))

-- Update metrics counters (metrics:total, metrics:allowed, metrics:rejected)
local metrics_key = KEYS[2]
redis.call('INCR', metrics_key .. ':total')
if allowed == 1 then
  redis.call('INCR', metrics_key .. ':allowed')
else
  redis.call('INCR', metrics_key .. ':rejected')
end

return {allowed, remaining, retry_after == nil and -1 or retry_after, now}
"""

# Sliding Window script (keys: 1) rl:sw:{user_id}:{policy_id}, 2) metrics counters key)
# ARGV: 1) limit, 2) window_ms, 3) current timestamp (ms)
SLIDING_WINDOW_SCRIPT = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

-- Remove stale entries
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
local allowed = 0
local remaining = 0
local retry_after = -1
if count < limit then
  allowed = 1
  remaining = limit - (count + 1)
  -- Use a UUID for member to avoid collisions
  local member = ARGV[4]
  redis.call('ZADD', key, now, member)
else
  allowed = 0
  remaining = 0
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')[2]
  retry_after = (oldest + window) - now
end

redis.call('EXPIRE', key, math.ceil(window / 1000))

-- Update metrics
local metrics_key = KEYS[2]
redis.call('INCR', metrics_key .. ':total')
if allowed == 1 then
  redis.call('INCR', metrics_key .. ':allowed')
else
  redis.call('INCR', metrics_key .. ':rejected')
end

return {allowed, remaining, retry_after, now}
"""

# Synchronization batch script (keys: 1) sync:processed_ids, 2) sync:processed_events, 3) sync:processed_batches, 4) base edge counter key prefix)
# ARGV: JSON array of events [{event_id, edge_id, allowed}]
SYNC_BATCH_SCRIPT = """
local processed_set = KEYS[1]
local events_key = KEYS[2]
local batches_key = KEYS[3]
local edge_prefix = KEYS[4]
local events_json = ARGV[1]
local events = cjson.decode(events_json)
local new_events = 0
for i, ev in ipairs(events) do
  local event_id = ev.event_id or ev.request_id
  if event_id and redis.call('SISMEMBER', processed_set, event_id) == 0 then
    redis.call('SADD', processed_set, event_id)
    local edge_key = edge_prefix .. ':edge:' .. ev.edge_id .. ':' .. (ev.allowed and 'allowed' or 'rejected')
    redis.call('INCR', edge_key)
    new_events = new_events + 1
  end
end
if new_events > 0 then
  redis.call('INCRBY', events_key, new_events)
  redis.call('INCR', batches_key)
end
return new_events
"""

LUA_SCRIPTS = {
    'TOKEN_BUCKET': TOKEN_BUCKET_SCRIPT,
    'SLIDING_WINDOW': SLIDING_WINDOW_SCRIPT,
    'SYNC_BATCH': SYNC_BATCH_SCRIPT,
}
