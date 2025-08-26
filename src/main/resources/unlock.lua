
--比较标识
if(redis.call('get',kEY[1])==ARGV[1]) then
    return redis.call('del',kEY[1])
    end
return 0