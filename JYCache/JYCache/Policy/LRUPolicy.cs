using System;
using System.Collections.Generic;
using System.Text;

/**
* 命名空间: JYCache 
* 类 名： LRUPolicy
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace JYCache
{
    /// <summary>
    /// 功能描述    ：LRUPolicy  
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/11/5 11:05:39 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/11/5 11:05:39 
    /// </summary>
    internal class LRUPolicy<T> : IPolicyCompare<T>
    {
        public int Compare(CacheEntity<T> element, CacheEntity<T> cache)
        {
            if(cache==null)
            {
                return 1;
            }
            if(element==null)
            {
                return -1;
            }
            return element.element.AcessTime.CompareTo(cache.element.AcessTime);
        }

       
    }
}
