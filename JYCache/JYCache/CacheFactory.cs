using System;
using System.Reflection;
using System.Threading;
using System.IO;
using System.Linq;

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
        private static int cacheID = 0;//标识
        private static Type CacheStore = null;
        /// <summary>
        /// 创建缓存
        /// </summary>
        /// <param name="config">缓存配置</param>
        /// <returns></returns>
        public static JYCache<TKey, TValue> Create(CacheConfig config = null)
        {
            if (config == null)
            {
                config = new CacheConfig();
            }
            if (config.WeakKey && typeof(TKey).IsValueType)
            {
                throw new Exception("TKey不能是值类型或者不能启用WeakKey");
            }
            if (config.WeakValue && typeof(TValue).IsValueType)
            {
                throw new Exception("TKey不能是值类型或者不能启用WeakKey");
            }
            JYCache<TKey, TValue> cache = new JYCache<TKey, TValue>(config);
            cache.CacheName = GetName();
            if(config.PersPolicy!=PersistencePolicy.None)
            {
                //反射加载
                if(CacheStore==null)
                {
                    string[] files = Directory.GetFiles(AppDomain.CurrentDomain.BaseDirectory);
                    if (files.Length > 0)
                    {
                        foreach (string file in files)
                        {
                            Assembly asm = Assembly.LoadFrom(file);
                            var types = asm.GetTypes().Where(t => t.GetInterfaces().Contains(typeof(ICacheStore.ICacheStore))).ToList();
                            if (types.Count > 0)
                            {
                                CacheStore = types[0];
                                break;
                            }

                        }
                    }

                }
                if(CacheStore!=null)
                {
                    ICacheStore.ICacheStore store=(ICacheStore.ICacheStore) Activator.CreateInstance(CacheStore);
                    cache.Store = store;
                    if(cache.Store!=null)
                    {
                        cache.Store.Name = cache.CacheName;
                        if (!string.IsNullOrEmpty(config.PersDir))
                        {
                            cache.Store.DataDir = config.PersDir;
                        }
                        cache.CheckStore();
                    }
                }
              
            }
            return cache;
        }

        /// <summary>
        /// 返回一个名称
        /// </summary>
        /// <returns></returns>
        private static string GetName()
        {
            return "cache_" + Interlocked.Increment(ref cacheID);
        }
    }
}
