using System;
using System.Collections.Generic;
using System.Text;

/**
* 命名空间: JYCache 
* 类 名： CacheCompare
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace JYCache
{
    /// <summary>
    /// 功能描述    ：CacheCompare  
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/11/6 11:42:28 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/11/6 11:42:28 
    /// </summary>
    internal class CacheCompare<T> : IComparer<CacheEntity<T>>
    {
        IPolicyCompare<T> policy = null;

        public CacheCompare(IPolicyCompare<T> compare)
        {
            policy = compare;
        }

        public int Compare(CacheEntity<T> x, CacheEntity<T> y)
        {
           return policy.Compare(x, y);
        }

    }
}
