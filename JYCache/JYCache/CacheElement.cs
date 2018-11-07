using System;
using System.Collections.Generic;
using System.Text;

/**
* 命名空间: JYCache 
* 类 名： CacheElement
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace JYCache
{
    /// <summary>
    /// 功能描述    ：CacheElement  
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/11/5 11:23:32 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/11/5 11:23:32 
    /// </summary>
   internal  class CacheElement<T>
    {
        public int ID { get; set; }

        /// <summary>
        ///元素
        /// </summary>
        public T Value { get; set; }

        /// <summary>
        /// 使用数
        /// </summary>
       public int Hit { get; set; }

        
        /// <summary>
        /// 最新写入时间
        /// </summary>
        public long WriteTime { get; set; }

        /// <summary>
        /// 最新访问时间
        /// </summary>
        public long AcessTime { get; set; }

       
    }
}
