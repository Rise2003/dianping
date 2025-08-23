package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOPTYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryType() {
        // 先检查是否存在缓存
        String allTypesKey = CACHE_SHOPTYPE_KEY + "all";
        String cachedJson = stringRedisTemplate.opsForValue().get(allTypesKey);

        //命中直接返回
        if (StrUtil.isNotBlank(cachedJson)) {
            List<ShopType> cachedTypes = JSONUtil.toList(cachedJson, ShopType.class);
            return Result.ok(cachedTypes);
        }


        //如果没命中Redis
        List<ShopType> typeList = query().orderByAsc("sort").list();
        typeList.stream()
                .forEach(type -> {
                    String shopTypeKey= CACHE_SHOPTYPE_KEY+type.getId();
                    stringRedisTemplate.opsForValue().set(shopTypeKey, JSONUtil.toJsonStr(type));
                });

        return Result.ok(typeList);
    }
}
