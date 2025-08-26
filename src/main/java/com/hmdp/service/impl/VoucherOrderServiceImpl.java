package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.*;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    // 初始化脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private BlockingQueue<VoucherOrder> voucherTask = new ArrayBlockingQueue<>(1024 * 1024);

    // 开启线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    // 使用volatile确保线程可见性
    private volatile IVoucherOrderService proxy;

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    // 开启异步线程
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取队列中的订单信息
                    VoucherOrder voucherOrder = voucherTask.take();
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }

        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            Long userId = voucherOrder.getUserId();
            // 创建锁对象
            RLock lock = redissonClient.getLock("lock:order:" + userId);

            try {
                // 获取锁，设置超时时间防止死锁
                boolean isLock = lock.tryLock(1, 10, TimeUnit.SECONDS);
                if (!isLock) {
                    log.error("获取锁失败，用户ID: {}", userId);
                    return;
                }

                try {
                    // 确保proxy不为null
                    if (proxy == null) {
                        log.error("代理对象未初始化");
                        return;
                    }
                    // 使用代理对象调用事务方法
                    proxy.createVoucherOrder(voucherOrder);
                    log.info("订单创建成功，订单ID: {}", voucherOrder.getId());
                } finally {
                    // 释放锁
                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                }
            } catch (InterruptedException e) {
                log.error("获取锁被中断", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("处理订单异常，订单ID: {}", voucherOrder.getId(), e);
            }
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("用户未登录");
        }

        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                user.getId().toString()
        );

        if (result == null) {
            return Result.fail("秒杀失败");
        }

        int r = result.intValue();
        // 2.判断结果不为0，没购买资格
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 3.创建订单对象
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(redisIdWorker.nextId("order"));
        voucherOrder.setUserId(user.getId());
        voucherOrder.setVoucherId(voucherId);

        // 4.添加到阻塞队列
        try {
            voucherTask.put(voucherOrder);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Result.fail("系统繁忙，请重试");
        }

        // 5.获取代理对象（必须在主线程中获取）
        if (proxy == null) {
            synchronized (this) {
                if (proxy == null) {
                    proxy = (IVoucherOrderService) AopContext.currentProxy();
                }
            }
        }

        // 6.返回订单id
        return Result.ok(voucherOrder.getId());
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        // 1.一人一单检查
        Integer count = query().eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        if (count > 0) {
            log.error("用户已经下过单，用户ID: {}, 优惠券ID: {}", userId, voucherId);
            return;
        }

        // 2.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();

        if (!success) {
            log.error("库存不足，优惠券ID: {}", voucherId);
            return;
        }

        // 3.保存订单
        boolean saveSuccess = save(voucherOrder);
        if (!saveSuccess) {
            log.error("保存订单失败，订单ID: {}", voucherOrder.getId());
            // 可以在这里考虑回滚库存
        } else {
            log.info("订单保存成功，订单ID: {}", voucherOrder.getId());
        }
    }
}