using System;
using System.Collections.Generic;
using System.Text;

/**
* 命名空间: JYCache 
* 类 名： SigleEntity
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace JYCache
{
    /// <summary>
    /// 功能描述    ：SigleEntity  
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/11/7 14:47:17 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/11/7 14:47:17 
    /// </summary>
   public class SigleEntity<TKey,TValue>
    {

        public TKey Key { get; set; }
        public long WriteTime { get; set; }

        /// <summary>
        /// 缓存时间毫秒保持
        /// </summary>
        public int CacheTime { get; set; }

        public ICache<TKey,TValue> Cache { get; set; }

         
    }
}
