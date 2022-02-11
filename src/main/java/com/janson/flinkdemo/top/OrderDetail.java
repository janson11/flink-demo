package com.janson.flinkdemo.top;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description: 订单详情
 * @Author: shanjian
 * @Date: 2022/2/11 3:22 下午
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderDetail {


    private Long userId;
    private Long itemId;
    private String citeName;
    private Double price;
    private Long timeStamp;

}
