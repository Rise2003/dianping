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

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
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

   /*
    @Override
    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠券
        SeckillVoucher vocher = seckillVoucherService.getById(voucherId);
        //2.判断秒杀是否开始
        if (vocher.getBeginTime().isAfter(LocalDateTime.now())) {
            //没开始
            return Result.fail("秒杀未开始");
        }
        //3.判断秒杀是否已将结束
        if (vocher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已结束");
        }
        //4.判断库存是否充足
        if (vocher.getStock()<1) {
            //库存不足
            return Result.fail("库存不足");
        }

        Long uId = UserHolder.getUser().getId();
        //创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + uId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否过去锁成功
        if (!isLock) {
            //获取失败
            return Result.fail("不允许重复下单");
        }

        try {
            //获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁lock.unlock();
        }

    }  */
   //初始化脚本
   private static  final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static { SECKILL_SCRIPT=new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private BlockingQueue<VoucherOrder> voucherTask=new ArrayBlockingQueue<>(1024 * 1024);
    //开启线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR= Executors.newSingleThreadExecutor();

    @PostConstruct//当前类初始化完成后执行
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    //开启异步线程
    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
        while (true) {
            try {
                //1.获取队列中的订单信息
                VoucherOrder voucherOrder = voucherTask.take();
                //2.创建订单
                handleVocherOrder(voucherOrder);
            } catch (InterruptedException e) {
                log.error("处理订单异常",e);
            }
        }
        }

        private void handleVocherOrder(VoucherOrder voucherOrder) {
            Long uId = voucherOrder.getUserId();
            //创建锁对象
            RLock lock = redissonClient.getLock("lock:order:" + uId);
            //获取锁
            boolean isLock = lock.tryLock();
            //判断是否过去锁成功
            if (!isLock) {
                //获取失败
                log.error("不允许重复下单");
                return;
            }

            try {
              proxy.createVoucherOrder(voucherOrder);

            } catch (IllegalStateException e) {
                throw new RuntimeException(e);
            } finally {
                //释放锁lock.unlock();
            }
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
       //获取用户
        UserDTO userId = UserHolder.getUser();
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        //2判断结果不为0，没购买资格
        if (result != 0) {
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }

        //2.1判断结果为0，保存下单信息
        //拿订单序列号
        VoucherOrder voucherOrder = new VoucherOrder();
        //2.2订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //2.3用户id
        Long uId = UserHolder.getUser().getId();
        voucherOrder.setUserId(uId);
        //2.4代金券id
        voucherOrder.setVoucherId(voucherId);

        //3.创建堵塞队列
        voucherTask.add(voucherOrder);

        //4.获取代理对象（事务）
        proxy = (IVoucherOrderService)AopContext.currentProxy();

        //5.返回订单id
        return Result.ok(orderId);

    }


    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //一人一单
        Long uId = voucherOrder.getUserId();


        //        查询订单
            Integer count = query().eq("user_id", uId).eq("voucher_id", voucherOrder.getVoucherId()).count();
            if (count > 0) {
                //用户已经下过单
                log.error("用户已经下过单");
                return;
            }

            //5.扣减库存
            boolean success = seckillVoucherService.update().setSql("stock=stock - 1")
                    .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                    .update();

            if (!success) {
                log.error("库存不足");
                return;
            }

            //6.创建订单

        }
}
