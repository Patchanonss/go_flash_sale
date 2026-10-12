package redis

// DeductStockLuaScript executes atomic check-and-decrement on Redis.
// KEYS[1]: item stock key (e.g., "item:stock:1")
// ARGV[1]: quantity to deduct
// Returns:
//   array [status, remaining_stock]
//   status: 1 (success), 0 (out of stock), -1 (key not found)
const DeductStockLuaScript = `
local stock_key = KEYS[1]
local quantity = tonumber(ARGV[1])

local current_stock = redis.call('get', stock_key)
if not current_stock then
    return {-1, 0}
end

current_stock = tonumber(current_stock)
if current_stock >= quantity then
    local remaining = current_stock - quantity
    redis.call('set', stock_key, remaining)
    return {1, remaining}
else
    return {0, current_stock}
end
`
