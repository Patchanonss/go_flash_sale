package redis

// DeductStockLuaScript executes atomic stock check, deduction, and optional idempotency registration.
// KEYS[1]: stock_key (e.g., "item:stock:1")
// KEYS[2]: idemp_key (e.g., "idemp:uuid")
// ARGV[1]: quantity to deduct (number)
// ARGV[2]: idemp_ttl in seconds (number)
// ARGV[3]: use_idemp ("1" or "0")
//
// Return status codes:
//   1: Success (stock deducted, idempotency key recorded)
//   2: Duplicate request (idempotency key already exists, zero deductions)
//   0: Out of stock (idempotency key NOT recorded, allowing future retries)
//  -1: Stock key not found in Redis
const DeductStockLuaScript = `
local stock_key = KEYS[1]
local idemp_key = KEYS[2]
local quantity = tonumber(ARGV[1])
local idemp_ttl = tonumber(ARGV[2])
local use_idemp = ARGV[3]

-- 1. Check idempotency if enabled
if use_idemp == "1" then
    local exists = redis.call('exists', idemp_key)
    if exists == 1 then
        return {2, 0}
    end
end

-- 2. Check stock existence
local current_stock = redis.call('get', stock_key)
if not current_stock then
    return {-1, 0}
end

current_stock = tonumber(current_stock)
if current_stock >= quantity then
    local remaining = current_stock - quantity
    redis.call('set', stock_key, remaining)
    if use_idemp == "1" then
        redis.call('setex', idemp_key, idemp_ttl, '1')
    end
    return {1, remaining}
else
    return {0, current_stock}
end
`

// RollbackStockLuaScript compensates stock if worker queue cannot accept the order.
// KEYS[1]: stock_key
// KEYS[2]: idemp_key
// ARGV[1]: quantity to restore
// ARGV[2]: use_idemp ("1" or "0")
const RollbackStockLuaScript = `
local stock_key = KEYS[1]
local idemp_key = KEYS[2]
local quantity = tonumber(ARGV[1])
local use_idemp = ARGV[2]

redis.call('incrby', stock_key, quantity)
if use_idemp == "1" then
    redis.call('del', idemp_key)
end
return 1
`
