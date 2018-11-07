using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

/**
* 命名空间: JYCache 
* 类 名： CacheFactory
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace JYCache
{
    /// <summary>
    /// 功能描述    ：CacheFactory  
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/11/6 17:26:20 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/11/6 17:26:20 
    /// </summary>
  public static  class CacheFactory<TKey,TValue>
    {
        private static int cacheID = 0;
       public static JYCache<TKey, TValue>  Create(CacheConfig config=null)
        {
            if(config==null)
            {
                config = new CacheConfig();
            }
            if(config.WeakKey&&typeof(TKey).IsValueType)
            {
                throw new Exception("TKey不能是值类型或者不能启用WeakKey");
            }
            if(config.WeakValue&&typeof(TValue).IsValueType)
            {
                throw new Exception("TKey不能是值类型或者不能启用WeakKey");
            }
            JYCache<TKey, TValue> cache = new JYCache<TKey, TValue>(config);
            cache.CacheName = GetName();
            return cache;
        }
        private static string GetName()
        {
            return "cache_" + Interlocked.Increment(ref cacheID);
        }
    }
}
