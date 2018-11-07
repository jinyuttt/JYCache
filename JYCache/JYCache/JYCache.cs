using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Threading;

/**
* 命名空间: JYCache 
* 类 名： JYCache
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace JYCache
{
    /// <summary>
    /// 功能描述    ：JYCache  缓存
    /// 如果允许扩展空间，则记录炒作顺序（非LFU）
    /// 如果允许扩展空间，LFU策略，则记录弱KEY,则记录KEY
    /// 如果不允许扩展空间，则记录KEY(获取KEY)
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/11/5 10:56:30 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/11/5 10:56:30 
    /// </summary>
    public class JYCache<TKey, TValue> : ICache<TKey, TValue>
    {
        private ConcurrentDictionary<TKey, CacheEntity<TValue>> dicCache = null;
        private ConditionalWeakTable<object, CacheEntity<TValue>> dicWeakKey = null;
        private ConcurrentDictionary<TKey, CacheEntity<WeakReference<object>>> dicWeakValue = null;
        private ConditionalWeakTable<object, CacheEntity<WeakReference<object>>> dicWeakKV = null;
         
        private ConcurrentStack<TKey> stack= null;//记录操作
        private ConcurrentDictionary<TKey, string> dicKeys = null;//记录所有key
        private ConcurrentQueue<RemoveEntity<TKey, TValue>> removeEntities = null;
        private IPolicyCompare<TValue> policy = null;
        private IPolicyCompare<WeakReference<object>> weakpolicy = null;
        private CacheConfig config = null;//配置
        private readonly int kvState = -1;//使用的类型
        private readonly int weighter = 0;//权重
        private volatile bool isRun = false;//正在消失数据
        private const int ticksMS = 10000;//1万个是1毫秒
        private bool isStop = false;//正在使用
        private long lastWriteTime = 0;//
        private long cacheSize = 0;//缓存大小
        private volatile bool isRunRemove = false;//启动推送;
        private volatile bool isRunTimeOut = false;//启动超时监测
        private int waitTimeOut = 0;//监测缓存时间休眠线程
        private Lazy<CacheTask<TKey,TValue>> lazyObject = new Lazy<CacheTask<TKey,TValue>>();

        /// <summary>
        /// 移除监听
        /// </summary>
        public event CacheEntityRemove<TKey, TValue> CacheRemoveListener;

        internal JYCache(CacheConfig cacheconfig)
        {
            this.config = cacheconfig;
            if(-1==config.ConcurrencyLevel)
            {
                config.ConcurrencyLevel = Environment.ProcessorCount * 2;
            }
            Contract.Assert(config.ConcurrencyLevel > 0);
            if (cacheconfig.WeakKey&& config.WeakValue)
            {
                kvState = 1;
                dicWeakKV = new ConditionalWeakTable<object, CacheEntity<WeakReference<object>>>();
            }
            else if(config.WeakKey)
            {
                kvState = 2;
                dicWeakKey = new ConditionalWeakTable<object, CacheEntity<TValue>>();
            }
            else if(config.WeakValue)
            {
                kvState = 3;
                dicWeakValue = new ConcurrentDictionary<TKey, CacheEntity<WeakReference<object>>>(config.ConcurrencyLevel, config.InitSzie);
            }
            else
            {
                kvState = 4;
                dicCache = new ConcurrentDictionary<TKey, CacheEntity<TValue>>(config.ConcurrencyLevel, config.InitSzie);
            }
            //
            weighter =(int) (config.MaxCacheSize * config.Weight);
            stack = new ConcurrentStack<TKey>();
            dicKeys = new ConcurrentDictionary<TKey, string>();
            removeEntities = new ConcurrentQueue<RemoveEntity<TKey, TValue>>();
            //策略
            switch(config.Policy)
            {
                case CachePolicy.FIFO:
                    policy = new FIFOPolicy<TValue>();
                    weakpolicy = new FIFOPolicy<WeakReference<object>>();
                    break;
                case CachePolicy.LFU:
                    policy = new LFUPolicy<TValue>();
                    weakpolicy = new LFUPolicy<WeakReference<object>>();
                    break;
                case CachePolicy.LRU:
                    policy = new LRUPolicy<TValue>();
                    weakpolicy = new LRUPolicy<WeakReference<object>>();
                    break;

            }
            // 启动定时器
            waitTimeOut = config.CacheTime * 1000;//转换毫秒
        }

        public string CacheName { get; set; }

        #region 内部操作

        /// <summary>
        ///推送移除元素
        /// </summary>
        private void CheckRemoveEntity()
        {
            Task.Factory.StartNew(() =>
            {

                Thread.Sleep(3000);
                while (!removeEntities.IsEmpty)
                {
                    RemoveEntity<TKey, TValue> entity = null;
                    if (removeEntities.TryDequeue(out entity))
                    {
                        if (CacheRemoveListener != null)
                        {
                            CacheRemoveListener(this, entity.Key, entity.Value, entity.State);
                        }
                    }
                    if (isStop)
                    {
                        break;
                    }
                }
                if(!isStop)
                {
                    CheckRemoveEntity();//递归更新线程
                }

            });
        }

        /// <summary>
        /// 定时监测过期
        /// </summary>
        private void CheckTimeOutThread()
        {
          
            Task.Factory.StartNew(() =>
            {

                if (waitTimeOut > 0)
                {
                    Thread.Sleep(waitTimeOut);
                    RemovePolicy();
                    long left = (DateTime.Now.Ticks - lastWriteTime) / ticksMS;//当前时间距离最早的写入时间；
                    waitTimeOut = config.CacheTime * 1000 - (int)left;//还剩余的时间
                    waitTimeOut = waitTimeOut > 0 ? waitTimeOut : 1000;
                }
               else
                {
                    Thread.Sleep(60000);//1分钟
                }
                if (!isStop)
                {
                    //轮换线程
                    CheckTimeOutThread();
                }

            });
        }
       
        
        /// <summary>
        /// 超时，弱引用移除，策略移除
        /// 通过定时器，操作触发
        /// 
        /// </summary>
        private void RemovePolicy()
        {
            //
            if (isRun)
            { return; }
            isRun = true;
            if(config.UseMemory&&(CachePolicy.FIFO==config.Policy||CachePolicy.LRU==config.Policy))
            {
                RemovePolicyMemory();
            }
            else
            {
                RemovePolicyNo();
            }
            isRun = false;
        }

        /// <summary>
        /// 监测策略
        /// </summary>
        private void  CheckPolicy()
        {
            if(cacheSize>weighter)
            {
                Task.Factory.StartNew(() =>
                {
                    RemovePolicy();
                });
            }
            if(!isRunRemove)
            {
                isRunRemove = true;
                CheckRemoveEntity();
            }
            if(!isRunTimeOut)
            {
                isRunTimeOut = true;
                CheckTimeOutThread();
            }
        }

        /// <summary>
        /// 移除
        /// </summary>
        private void RemovePolicyNo()
        {
           
            //获取所有Key
           
            LinkedList<TKey> link = null;
            if(dicKeys.Count>0)
            {
                //记录了Key;
                link = new LinkedList<TKey>(dicKeys.Keys);
            }
            else
            {
                if(3==kvState)
                {
                    link = new LinkedList<TKey>(dicWeakValue.Keys);
                }
                else if(4==kvState)
                {
                    link = new LinkedList<TKey>(dicCache.Keys);
                }
            }
            WeakRef(link);
            CheckCacheTime(link);
            //
            CacheEntity<WeakReference<object>> entityweak = null;
            CacheEntity<WeakReference<object>>[] cacheWeakEntity = null;
            CacheEntity<TValue> entity = null;
            CacheEntity<TValue>[] cacheEntities = null;
            Dictionary<CacheEntity<WeakReference<object>>, TKey> dicWeak = new Dictionary<CacheEntity<WeakReference<object>>, TKey>();
            Dictionary<CacheEntity<TValue>, TKey> dic = new Dictionary<CacheEntity<TValue>, TKey>();
            int index = 0;
            switch (kvState)
            {
                case 1:
                    {
                        cacheWeakEntity = new CacheEntity<WeakReference<object>>[link.Count];
                        dicWeak = new Dictionary<CacheEntity<WeakReference<object>>, TKey>();
                        foreach (TKey tkey in link)
                        {
                            if (dicWeakKV.TryGetValue(tkey, out entityweak))
                            {
                                cacheWeakEntity[index++] = entityweak;
                                dicWeak[entityweak] = tkey;
                            }
                        }
                        //
                        List<CacheEntity<WeakReference<object>>> list = SortKeys(cacheWeakEntity);
                        this.RemovePolicyList(list, dicWeak);
                        cacheWeakEntity = null;
                        list.Clear();
                        dicWeak.Clear();
                    }
                    break;
                case 3:
                    {
                        cacheWeakEntity = new CacheEntity<WeakReference<object>>[link.Count];
                        dicWeak = new Dictionary<CacheEntity<WeakReference<object>>, TKey>();
                        foreach (TKey tkey in link)
                        {
                            if (dicWeakValue.TryGetValue(tkey, out entityweak))
                            {
                                cacheWeakEntity[index++] = entityweak;
                                dicWeak[entityweak] = tkey;
                            }
                        }
                        //
                        List<CacheEntity<WeakReference<object>>> list = SortKeys(cacheWeakEntity);
                        this.RemovePolicyList(list, dicWeak);
                        list.Clear();
                        dicWeak.Clear();
                        cacheWeakEntity = null;
                    }
                    break;
                case 2:
                    {
                        cacheEntities = new CacheEntity<TValue>[link.Count];
                        foreach (TKey tkey in link)
                        {
                            if (dicWeakKey.TryGetValue(tkey, out entity))
                            {
                                cacheEntities[index++] = entity;
                                dic[entity] = tkey;
                            }
                        }
                        List<CacheEntity<TValue>> list = SortKeys(cacheEntities);
                        this.RemovePolicyList(list, dic);
                        dicWeak.Clear();
                        list.Clear();
                        cacheEntities = null;
                    }
                    break;
                case 4:
                    {
                        cacheEntities = new CacheEntity<TValue>[link.Count];
                        foreach (TKey tkey in link)
                        {
                            if (dicCache.TryGetValue(tkey, out entity))
                            {
                                cacheEntities[index++] = entity;
                                dic[entity] = tkey;
                            }
                        }
                        //
                        List<CacheEntity<TValue>> list = SortKeys(cacheEntities);
                        this.RemovePolicyList(list, dic);
                        //
                        list.Clear();
                        dic.Clear();
                        cacheEntities = null;
                    }
                    break;

                default:
                    break;
                  

            }
            link.Clear();
        }

        /// <summary>
        /// 移除元素
        /// 按照设置的最大数据及权重
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <param name="dic"></param>
        private void RemovePolicyList<T>(List<T> list,Dictionary<T,TKey> dic)
        {
        
            int num = list.Count - weighter;

            //通过list删除
            TKey key;
            TValue value;
            for (int i = 0; i < num; i++)
            {
                if (i >= list.Count - 1)
                {
                    break;
                }
                if (dic.TryGetValue(list[i], out key))
                {
                    if (TryRemove(key, out value))
                    {
                        RemoveEntity<TKey, TValue> element = new RemoveEntity<TKey, TValue>() { Key = key, State = 3, Value = value };
                        removeEntities.Enqueue(element);
                        if(IsPersistent(key))
                        {
                            lazyObject.Value.RemovePersistent(key);
                        }
                    }
                    else
                    {
                        num++;
                    }
                }
                else
                {
                    num++;
                }
            }
        }


        /// <summary>
        /// 允许空间换时间
        /// 记录操作：允许扩展空间并且是FIFO，LRU策略
        /// </summary>
        private void RemovePolicyMemory()
        {
            //
           
            //全面用空间换时间
            //所有key
            Dictionary<TKey, string> dic = new Dictionary<TKey, string>();//过滤
            //所有Key顺序
            LinkedList<TKey> list = new LinkedList<TKey>();
            TKey[] tkeys = new TKey[stack.Count];
           //要保障顺序，不能用多线程
            int r = stack.TryPopRange(tkeys);
            for (int i = 0; i < r; i++)
            {
                if (!dic.ContainsKey(tkeys[i]))
                {
                    list.AddLast(tkeys[i]);
                }
                else
                {
                    dic[tkeys[i]] = null;
                }
              
            }
            //没有记录KEY,修正一下数据量
            cacheSize = dic.Count;
            //
            WeakRef(list);
            //超时
            CheckCacheTime(list);
            //FIFO,LRU
            TValue value = default(TValue);
            if (list.Count > weighter)
            {
                //
                int num = list.Count - weighter;
                do
                {
                    //通过list删除
                    if (TryRemove(list.Last.Value, out value))
                    {
                        RemoveEntity<TKey, TValue> element = new RemoveEntity<TKey, TValue>() { Key = list.Last.Value, State = 3 };
                        removeEntities.Enqueue(element);
                        num--;
                    }
                    list.RemoveLast();
                    if(list.Count==0)
                    {
                        break;
                    }
                   
                } while (num > 0);
            }
        }

        /// <summary>
        /// 元素排序
        /// </summary>
        /// <param name="allValue"></param>
        /// <returns></returns>
        private List<CacheEntity<TValue>> SortKeys(CacheEntity<TValue>[] allValue)
        {
            lock (this)
            {
                List<CacheEntity<TValue>> values = new List<CacheEntity<TValue>>(allValue);
                values.Sort(new CacheCompare<TValue>(policy));
                return values;
            }
        }

        /// <summary>
        /// 元素排序
        /// </summary>
        /// <param name="allValue"></param>
        /// <returns></returns>
        private List<CacheEntity<WeakReference<object>>> SortKeys(CacheEntity<WeakReference<object>>[] allValue)
        {
            List<CacheEntity<WeakReference<object>>> values = new List<CacheEntity<WeakReference<object>>>(allValue);
            values.Sort(new CacheCompare<WeakReference<object>>(weakpolicy));
            return values;
        }

       

        /// <summary>
        /// 监测超时
        /// </summary>
        /// <param name="keys"></param>
        private void CheckCacheTime(LinkedList<TKey> keys)
        {
            if(4!=kvState)
            {
                return;
            }
            //
            CacheEntity<TValue> entity = null;
            long curTime = DateTime.Now.Ticks;
            lastWriteTime = long.MaxValue;
            int leftTime = config.CacheTime * 1000;
            long timeLen = 0;
            Parallel.ForEach(keys, (key) =>
            {
                if (!IsPersistent(key))
                {
                    //非持久元素
                    if (dicCache.TryGetValue(key, out entity))
                    {
                        timeLen = curTime - entity.element.WriteTime;
                        if (timeLen / ticksMS >= leftTime)
                        {
                            //
                            RemoveEntity<TKey, TValue> element = new RemoveEntity<TKey, TValue>() { Key = key, State = 1 };
                            element.Value = entity.element.Value;
                            removeEntities.Enqueue(element);
                        }
                        else
                        {
                            if (lastWriteTime > entity.element.WriteTime)
                            {
                                //记录最早的写入
                                lastWriteTime = entity.element.WriteTime;
                            }
                        }

                    }
                }
            });
        }


       /// <summary>
       /// 监测弱引用，包括超时到期
       /// </summary>
       /// <param name="keys"></param>
        private void WeakRef(LinkedList<TKey> keys)
        {
            if (4 == kvState)
            {
                return;
            }
            object item = null;
            long curTime = DateTime.Now.Ticks;
            TValue value = default(TValue);
            CacheEntity<WeakReference<object>> kv = null;
            CacheEntity<TValue> entity = null;
            lastWriteTime = long.MaxValue;
            int leftTime = config.CacheTime * 1000;
            Parallel.ForEach(keys, (key) =>
            {
                if (!IsPersistent(key))
                {
                    //不是持久Key才判断
                    switch (kvState)
                    {
                        case 1:
                            {
                                if (dicWeakKV.TryGetValue(key, out kv))
                                {
                                    if (!kv.element.Value.TryGetTarget(out item))
                                    {
                                        //没有数据了
                                        dicWeakKV.Remove(key);
                                        RemoveEntity<TKey, TValue> element = new RemoveEntity<TKey, TValue>() { Key = key, State = 2 };
                                        removeEntities.Enqueue(element);
                                    }
                                    else if ((curTime - kv.element.WriteTime) / ticksMS > leftTime)
                                    {
                                        //
                                        RemoveEntity<TKey, TValue> element = new RemoveEntity<TKey, TValue>() { Key = key, State = 1 };
                                        if (TryRemove(key, out value))
                                        {
                                            element.Value = value;
                                        }
                                        removeEntities.Enqueue(element);
                                    }
                                    else
                                    {
                                        if (lastWriteTime > kv.element.WriteTime)
                                        {
                                            //记录最早的写入
                                            lastWriteTime = kv.element.WriteTime;
                                        }
                                    }
                                }
                            }
                            break;
                        case 3:
                            {
                                if (dicWeakValue.TryGetValue(key, out kv))
                                {
                                    if (!kv.element.Value.TryGetTarget(out item))
                                    {
                                        //没有数据了
                                        dicWeakValue.TryRemove(key, out kv);
                                        RemoveEntity<TKey, TValue> element = new RemoveEntity<TKey, TValue>() { Key = key, State = 2 };
                                        removeEntities.Enqueue(element);
                                    }
                                    else if ((curTime - kv.element.WriteTime) / ticksMS > leftTime)
                                    {
                                        if (lazyObject.IsValueCreated)
                                        {
                                            //是否是持久缓存元素
                                            if (!lazyObject.Value.IsPersistent(key))
                                            {
                                                RemoveEntity<TKey, TValue> element = new RemoveEntity<TKey, TValue>() { Key = key, State = 1 };
                                                if (TryRemove(key, out value))
                                                {
                                                    element.Value = value;
                                                }
                                                removeEntities.Enqueue(element);

                                            }
                                        }
                                        else
                                        {

                                            //
                                            RemoveEntity<TKey, TValue> element = new RemoveEntity<TKey, TValue>() { Key = key, State = 1 };
                                            if (TryRemove(key, out value))
                                            {
                                                element.Value = value;
                                            }
                                            removeEntities.Enqueue(element);
                                        }

                                    }
                                    if (lastWriteTime > kv.element.WriteTime)
                                    {
                                        //记录最早的写入
                                        lastWriteTime = kv.element.WriteTime;
                                    }
                                }
                            }
                            break;
                        case 2:
                            {
                                if (dicWeakKey.TryGetValue(key, out entity))
                                {
                                    //值检查超时
                                    if ((curTime - entity.element.WriteTime) / ticksMS > leftTime)
                                    {
                                        //
                                        RemoveEntity<TKey, TValue> element = new RemoveEntity<TKey, TValue>() { Key = key, State = 1 };
                                        if (TryRemove(key, out value))
                                        {
                                            element.Value = value;
                                        }
                                        removeEntities.Enqueue(element);
                                    }
                                    if (lastWriteTime > entity.element.WriteTime)
                                    {
                                        //记录最早的写入
                                        lastWriteTime = entity.element.WriteTime;
                                    }
                                }
                            }
                            break;
                    }
                }

            });
                
            
        }

        /// <summary>
        /// 是否是持久的元素
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        private bool IsPersistent(TKey key)
        {
           return lazyObject.IsValueCreated &&lazyObject.Value.IsPersistent(key);
        }
        #endregion

        #region 接口操作
        public void Add(TKey key, TValue value,int slfTime=-1)
        {
            if (lazyObject.IsValueCreated)
            {
                lazyObject.Value.Refresh(key);
            }
            if (slfTime >-1)
            {
                SigleEntity<TKey, TValue> sigle = new SigleEntity<TKey, TValue>() { Cache = this, CacheTime = slfTime * 1000, Key = key, WriteTime = DateTime.Now.Ticks };
                lazyObject.Value.Add(sigle);
            }
            switch (kvState)
            {
                case 1:
                    {
                        CacheEntity<WeakReference<object>> entityKV = null;
                        object outValue = (object)value;
                        if (dicWeakKV.TryGetValue(key, out entityKV))
                        {
                            //更新
                            entityKV.element.WriteTime = DateTime.Now.Ticks;
                            entityKV.element.AcessTime = DateTime.Now.Ticks;
                            entityKV.element.Value = new WeakReference<object>(outValue);
                           
                        }
                        else
                        {
                            object outKey = (object)key;
                            entityKV = new CacheEntity<WeakReference<object>>();
                            entityKV.element = new CacheElement<WeakReference<object>>();
                            entityKV.element.AcessTime = DateTime.Now.Ticks;
                            entityKV.element.Hit = 0;
                            entityKV.element.Value = new WeakReference<object>(outValue);
                            entityKV.element.WriteTime = DateTime.Now.Ticks;
                            dicWeakKV.Add(outKey, entityKV);
                            if (config.UseMemory && (config.Policy == CachePolicy.FIFO || config.Policy == CachePolicy.LRU))
                            {
                                stack.Push(key);//记录操作
                            }
                            else
                            {
                                dicKeys[key] = null;//必须记录KEY
                            }
                            //
                           
                            Interlocked.Increment(ref cacheSize);
                        }
                    }
                    break;
                case 2:
                    {
                        CacheEntity<TValue> entityK = null;
                        if (dicWeakKey.TryGetValue(key, out entityK))
                        {
                            entityK.element.WriteTime = DateTime.Now.Ticks;
                            entityK.element.AcessTime = DateTime.Now.Ticks;
                            entityK.element.Value = value;
                        }
                        else
                        {
                            object outK = (object)key;
                            entityK = new CacheEntity<TValue>();
                            entityK.element = new CacheElement<TValue>();
                            entityK.element.AcessTime = DateTime.Now.Ticks;
                            entityK.element.Hit = 0;
                            entityK.element.WriteTime = DateTime.Now.Ticks;
                            entityK.element.Value = value;
                            dicWeakKey.Add(outK, entityK);
                            if (config.UseMemory && (config.Policy == CachePolicy.FIFO || CachePolicy.LRU == config.Policy))
                            {
                                stack.Push(key);
                            }
                            else
                            {
                                dicKeys[key] = null;
                            }
                            Interlocked.Increment(ref cacheSize);
                        }
                    }
                    break;
                case 3:
                    {
                        CacheEntity<WeakReference<object>> entityV = null;
                        object outV = (object)value;
                        if (dicWeakValue.TryGetValue(key, out entityV))
                        {
                            entityV.element.WriteTime = DateTime.Now.Ticks;
                            entityV.element.AcessTime = DateTime.Now.Ticks;
                            entityV.element.Value = new WeakReference<object>(outV);
                        }
                        else
                        {
                            entityV = new CacheEntity<WeakReference<object>>();
                            entityV.element = new CacheElement<WeakReference<object>>();
                            entityV.element.AcessTime = DateTime.Now.Ticks;
                            entityV.element.Hit = 0;
                            entityV.element.Value = new WeakReference<object>(outV);
                            entityV.element.WriteTime = DateTime.Now.Ticks;
                            dicWeakValue[key] = entityV;
                            if (config.UseMemory)
                            {
                                stack.Push(key);
                            }
                            Interlocked.Increment(ref cacheSize);
                        }
                    }
                    break;
                case 4:
                    {
                        CacheEntity<TValue> entity = null;
                        if (dicCache.TryGetValue(key, out entity))
                        {
                            entity.element.WriteTime = DateTime.Now.Ticks;
                            entity.element.AcessTime = DateTime.Now.Ticks;
                            entity.element.Value = value;
                        }
                        else
                        {
                            entity = new CacheEntity<TValue>();
                            entity.element = new CacheElement<TValue>();
                            entity.element.AcessTime = DateTime.Now.Ticks;
                            entity.element.WriteTime = DateTime.Now.Ticks;
                            entity.element.Hit =0;
                            entity.element.Value = value;
                            dicCache[key] = entity;
                            if (config.UseMemory)
                            {
                                stack.Push(key);//记录操作
                            }
                            Interlocked.Increment(ref cacheSize);
                        }
                    }
                    break;
            }
            CheckPolicy();
        }

        public void Clear()
        {
            switch (kvState)
            {
                case 1:

                    dicWeakKV = new ConditionalWeakTable<object, CacheEntity<WeakReference<object>>>();
                    break;
                case 2:
                    dicWeakKey = new ConditionalWeakTable<object, CacheEntity<TValue>>();
                    break;
                case 3:
                    dicWeakValue.Clear();
                    break;
                case 4:
                    dicCache.Clear();
                    break;
            }
        }

        public List<TValue> GetCaches(TKey[] keys)
        {
           if(null==keys||0==keys.Length)
            {
                return null;
            }
            TValue value;
            List<TValue> list = new List<TValue>(keys.Length);
            foreach(TKey key in keys)
            {
                if(TryGetCache(key,out value))
                {
                    list.Add(value);
                }
            }
            return list;
        }

        public void InvalidateAll(TKey[] keys)
        {
            if (null == keys || 0 == keys.Length)
            {
                return ;
            }
            foreach(TKey key in keys)
            {
                Remove(key);
            }
        }

        public void Remove(TKey key)
        {
            TValue value = default(TValue);
            TryRemove(key, out value);
        }

        public bool TryGetCache(TKey key, out TValue value)
        {
              bool r = false;
             value = default(TValue);
            object v = null;
            switch (kvState)
            {
                case 1:
                    {
                        CacheEntity<WeakReference<object>> entityKV = null;
                        if (dicWeakKV.TryGetValue(key, out entityKV))
                        {
                            if (entityKV.element.Value.TryGetTarget(out v))
                            {
                                value = (TValue)v;
                                entityKV.element.AcessTime = DateTime.Now.Ticks;
                                entityKV.element.Hit++;
                                r = true;
                            }
                            else
                            {
                                //如果value回收
                                dicWeakKV.Remove(key);
                            }
                        }
                    }
                    break;
                case 2:
                    {
                        CacheEntity<TValue> entityK = null;
                        if (dicWeakKey.TryGetValue(key, out entityK))
                        {
                            value = entityK.element.Value;
                            entityK.element.AcessTime = DateTime.Now.Ticks;
                            entityK.element.Hit++;
                            r = true;
                        }
                    }
                    break;
                case 3:
                    {
                        CacheEntity<WeakReference<object>> entityV = null;
                        if (dicWeakValue.TryGetValue(key, out entityV))
                        {
                            if (entityV.element.Value.TryGetTarget(out v))
                            {
                                value = (TValue)v;
                                entityV.element.AcessTime = DateTime.Now.Ticks;
                                entityV.element.Hit++;
                                r = true;
                            }
                            else
                            {
                                dicWeakValue.TryRemove(key, out entityV);
                            }
                        }
                    }
                    break;
                case 4:
                    {
                        CacheEntity<TValue> entity = null;
                        if (dicCache.TryGetValue(key, out entity))
                        {
                            value = entity.element.Value;
                            entity.element.AcessTime = DateTime.Now.Ticks;
                            entity.element.Hit++;
                            r = true;
                        }
                    }
                    break;
            }
            if(CachePolicy.LRU==config.Policy&&config.UseMemory)
            {
                stack.Push(key);
            }
            CheckPolicy();
            return r;
        }

        private bool TryRemove(TKey key,out TValue value)
        {
            value = default(TValue);
            bool result = false;
            CacheEntity<WeakReference<object>> kv = null;
            CacheEntity<TValue> entity = null;
            string item = null;
            switch (kvState)
            {
                case 1:
                    if (dicWeakKV.TryGetValue(key, out kv))
                    {
                        dicWeakKV.Remove(key);
                        result = true;
                        Interlocked.Decrement(ref cacheSize);
                    }
                    else
                    {
                        //如果记录了KEY
                        if(dicKeys.TryRemove(key,out item))
                        {
                            Interlocked.Decrement(ref cacheSize);
                        }
                    }
                   
                    break;
                case 2:
                   if(dicWeakKey.TryGetValue(key,out entity))
                    {
                        dicWeakKey.Remove(key);
                        result = true;
                        Interlocked.Decrement(ref cacheSize);
                    }
                    else
                    {
                        //如果记录了KEY
                        if (dicKeys.TryRemove(key, out item))
                        {
                            Interlocked.Decrement(ref cacheSize);
                        }
                    }
                    break;
                case 3:
                    if (dicWeakValue.TryRemove(key, out kv))
                    {
                        result = true;
                        Interlocked.Decrement(ref cacheSize);
                    }
                    break;
                case 4:
                   if(dicCache.TryRemove(key, out entity))
                    {
                        result = true;
                        Interlocked.Decrement(ref cacheSize);
                    }
                    break;
            }
            CheckPolicy();
            return result;
        }

        #endregion
    }
}
