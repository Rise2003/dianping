package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
@Resource
private StringRedisTemplate stringRedisTemplate;
@Resource
private IUserService userService;
    @Override
    public Result follow(Long followUserId, boolean isFollow) {
        //获取登录用户
        Long userId = UserHolder.getUser().getId();
        String key="follow:"+userId;
        //1.判断是否关注
        if (isFollow) {
            //2.关注，新增数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);

            if (isSuccess) {
                //把关注用户的id,放入redis的set集合

                stringRedisTemplate.opsForSet().add(key,followUserId.toString());
            }
        }
        else {
            //3.取关，删除
            boolean isSuccess = remove(new QueryWrapper<Follow>().eq("user_id", userId)
                    .eq("follow_user_id", followUserId));
            //从redis集合中移除
            if (isSuccess) {
                stringRedisTemplate.opsForSet().remove(key,followUserId.toString());
            }
        }



        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        //1.查询是否关注
        Integer count = query().eq("follow_user_id", followUserId)
                .eq("user_id", UserHolder.getUser().getId()).count();
        //判断


        return Result.ok(count >0);
    }

    @Override
    public Result followCommons(Long id) {
        //获取当前用户
        Long userId = UserHolder.getUser().getId();
        String key="follow:"+userId;
        //目标用户
        String key2="follow:"+id;
        //求交集
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);

        if (intersect==null || intersect.isEmpty()) {
            //无交集
            return Result.ok(Collections.emptyList());
        }

                //解析id集合
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());

        //查询用户
        List<User> users = userService.listByIds(ids);

        //数据转换
        List<UserDTO> userDTOS = users.stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }
}
