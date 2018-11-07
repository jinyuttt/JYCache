using System;
using System.Collections.Generic;
using System.Text;
using System;
using System.ComponentModel;
/**
* 命名空间: JYCache 
* 类 名： CachePolicy
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace JYCache
{
    /// <summary>
    /// 功能描述    ：CachePolicy  缓存移除策略
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/11/5 10:30:33 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/11/5 10:30:33 
    /// </summary>
    public enum CachePolicy
    {

        /// <summary>
        /// 最近最少使用
        /// </summary>
        [Description("最近最少使用")]
        LRU,

        /// <summary>
        /// 先进先出
        /// </summary>
        [Description("先进先出")]
        FIFO,

        /// <summary>
        /// 最少使用
        /// </summary>
        [Description("最少使用")]
        LFU,

        

        /// <summary>
        /// 自定义
        /// </summary>
        [Description("自定义")]
        CUSTOM,

        /// <summary>
        /// 未知
        /// </summary>
        [Description("未知")]
        UNKNOW
    }
}
