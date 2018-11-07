using System.Collections.Generic;

namespace JYCache
{
    public delegate void CacheEntityRemove<TKey, TValue>(object sender, TKey key, TValue value,int state);
    public interface ICache<TKey,TValue>
    {
        event CacheEntityRemove<TKey, TValue> CacheRemoveListener;

        /// <summary>
        /// Cache名称
        /// </summary>
        string CacheName { get; set; }

       /// <summary>
       /// 添加元素
       /// </summary>
       /// <param name="key">key</param>
       /// <param name="value">value</param>
       /// <param name="sflCacheTime">保持时间，默认安装配置，标识用不消除，大于0标识秒</param>
        void Add(TKey key, TValue value,int sflTime=-1);

        /// <summary>
        /// 移除数据
        /// </summary>
        /// <param name="key"></param>
        void Remove(TKey key);

        /// <summary>
        /// 批量移除
        /// </summary>
        /// <param name="keys"></param>
        void InvalidateAll(TKey[] keys);

      

        /// <summary>
        /// 获取
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool TryGetCache(TKey key,out TValue value);

        /// <summary>
        /// 批量获取
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        List<TValue> GetCaches(TKey[] keys);

        /// <summary>
        /// 清除
        /// </summary>
        void Clear();


    }
}
