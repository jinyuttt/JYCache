using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace JYCache
{
    internal class PersCache<TKey,TValue>
    {
        int maxSize = 10000;
        private readonly ConcurrentDictionary<TKey, PersItem<TKey,TValue>> dicCache = null;
        private int num = 0;

        public int MaxSize { get { return maxSize; } set { maxSize = value; } }

        public void Add(TKey key,TValue value)
        {
            dicCache[key] = new PersItem<TKey,TValue>(key,value);
            if(Interlocked.Increment(ref num)>MaxSize)
            {
                Task.Factory.StartNew(() =>
                {
                    Check();
                });
            }
        }

        public bool TryGetValue(TKey key,out TValue value)
        {
            PersItem<TKey,TValue> tmp=null;
            value = default(TValue);
            if (dicCache.TryGetValue(key, out tmp))
            {
                value = tmp.GetValue();
                return true;
            }
            return false;
        }

        private void Check()
        {
            PersItem<TKey, TValue> item;
            List<PersItem<TKey, TValue>> lst = new List<PersItem<TKey, TValue>>(MaxSize);
            lst.AddRange(dicCache.Values);
            lst.Sort((x, y) => x.Rate.CompareTo(y.Rate));
            int num =(int) (MaxSize * 0.1);
            for(int i=0;i<num;i++)
            {
                if (dicCache.TryRemove(lst[i].Key, out item))
                {
                    Interlocked.Decrement(ref num);
                }
            }
        }
    }

    internal class PersItem<TKey,TValue>
    {
        private int Num = 0;
        private TValue value;
        public TKey Key { get; set; }

        private readonly long Create = DateTime.Now.Ticks;

        public double Rate
        {
            get { return Num / ((DateTime.Now.Ticks - Create) / 10000000); }
        }

        public PersItem(TKey key,  TValue value)
        {
            this.value = value;
            Key = key;
        }

        public TValue GetValue()
        {
            Num++;
            return value;
        }

    }
}
