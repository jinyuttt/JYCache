using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
/**
* 命名空间: JYCache 
* 类 名： CahcheTask
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace JYCache
{
    /// <summary>
    /// 功能描述    ：CahcheTask  管理单独的缓存对象
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/11/7 14:37:13 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/11/7 14:37:13 
    /// </summary>
  public  class CacheTask<TKey,TValue>
    {
        private  ICache<TKey,TValue> cache=null;
        private volatile bool isRun = false;
        private volatile int minCacheTime = int.MaxValue;
        private volatile int nextCacheTime = int.MaxValue;
        private const int ticksMS = 10000;//1万个是1毫秒
        private ConcurrentQueue<SigleEntity<TKey, TValue>> queue = null;
        private ConcurrentDictionary<TKey, long> pairs;//更新写入时间
        private ConcurrentDictionary<TKey, string> persistence = null;//持久化
        public CacheTask()
        {
            
            queue = new ConcurrentQueue<SigleEntity<TKey, TValue>>();
            pairs = new ConcurrentDictionary<TKey, long>();
            persistence = new ConcurrentDictionary<TKey, string>();

        }
      

        /// <summary>
        /// 添加元素
        /// </summary>
        /// <param name="entity"></param>
        public void Add(SigleEntity<TKey,TValue> entity)
        {
            queue.Enqueue(entity);
            if (entity.CacheTime > 0)
            {
                pairs[entity.Key] = entity.WriteTime;
                if (minCacheTime > entity.CacheTime)
                {
                    minCacheTime = entity.CacheTime;
                }
            }
            else if(entity.CacheTime==0)
            {
                persistence[entity.Key] = null;
            }
            
        }

        /// <summary>
        /// 刷新时间
        /// </summary>
        /// <param name="key"></param>
        public void Refresh(TKey key)
        {
            //写入时间更新
            if (pairs.ContainsKey(key))
            {
                pairs[key] = DateTime.Now.Ticks;
            }
        }

        /// <summary>
        /// 是否是持久保持
        /// 只受缓存最大量约束，不受配置时间约束
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool IsPersistent(TKey key)
        {
            return persistence.ContainsKey(key);
        }

        /// <summary>
        /// 最大量移除时移除保持数据
        /// </summary>
        /// <param name="key"></param>
        public void RemovePersistent(TKey key)
        {
            string item;
            persistence.TryRemove(key, out item);
        }

        /// <summary>
        /// 监测缓存时间
        /// </summary>
        private void Take()
        {
            if(isRun)
            {
                return;
            }
            isRun = true;
            Task.Factory.StartNew(() =>
            {
                Thread.Sleep(2000);//延迟2秒
                Thread.Sleep(minCacheTime / 2);
                int num = queue.Count;
                if(num>0)
                {
                    SigleEntity<TKey,TValue> entity = null;
                    long writeTime = 0;
                    long curTime = DateTime.Now.Ticks;
                    do
                    {
                        if (queue.TryDequeue(out entity))
                        {
                            if(!pairs.TryGetValue(entity.Key,out writeTime))
                            {
                                writeTime = entity.WriteTime;
                            }
                            //
                            if ((curTime - writeTime) / ticksMS > entity.CacheTime)
                            {
                                //移除
                                cache.Remove(entity.Key);
                                pairs.TryRemove(entity.Key, out writeTime);
                                entity.Cache.Remove(entity.Key);
                            }
                            else
                            {
                                if (nextCacheTime > entity.CacheTime)
                                {
                                    nextCacheTime = entity.CacheTime;
                                }
                            }

                        }
                        num--;
                    } while (num > 0);
                    //
                    minCacheTime = nextCacheTime;
                    nextCacheTime = int.MaxValue;
                    isRun = false;
                }

            });
        }
    }
}
