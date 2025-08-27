local voucherId = ARGV[1]
local userId = ARGV[2]
--订单Id
local orderId=ARGV[3]

-- Key
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- 脚本业务
-- 判断库存是否充足
if (tonumber(redis.call('get', stockKey)) <= 0) then
    return 1
end

-- 判断用户是否已经下单
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

-- 扣库存（正确语法）
redis.call('incrby', stockKey, -1)

-- 下单（保存用户）
redis.call('sadd', orderKey, userId)

--发送消息到队列
redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)
return 0