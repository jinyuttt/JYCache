using System;
using System.Collections.Generic;
using System.Text;

/**
* 命名空间: JYCache 
* 类 名： LFRPolicy
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace JYCache
{
    /// <summary>
    /// 功能描述    ：LFRPolicy  
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/11/5 11:14:19 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/11/5 11:14:19 
    /// </summary>
    internal sealed class LFUPolicy<T> : IPolicyCompare<T>
    {
        public int Compare(CacheEntity<T> element, CacheEntity<T> cache)
        {
            return element.element.Hit.CompareTo(cache.element.Hit);
        }
    }
}
