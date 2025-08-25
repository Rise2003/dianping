package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
@Resource
private ISeckillVoucherService seckillVoucherService;
@Resource
private RedisIdWorker redisIdWorker;
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

        //加锁
        Long uId = UserHolder.getUser().getId();
        synchronized (uId.toString().intern()) {
            //获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }
    }

    @Transactional
    public  Result createVoucherOrder(Long voucherId) {
        //一人一单
        Long uId = UserHolder.getUser().getId();


            //        查询订单
            Integer count = query().eq("user_id", uId).eq("voucher_id", voucherId).count();
            if (count > 0) {
                //用户已经下过单
                return Result.fail("该用户已经购买过");
            }

            //5.扣减库存
            boolean success = seckillVoucherService.update().setSql("stock=stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0)
                    .update();

            if (!success) {
                return Result.fail("库存不足");
            }

            //6.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            //订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            //用户id
            Long userId = UserHolder.getUser().getId();
            voucherOrder.setUserId(userId);
            //代金券id
            voucherOrder.setVoucherId(voucherId);
            //7.返回订单
            save(voucherOrder);
            return Result.ok(orderId);
        }
}
