using System;
using System.Collections.Generic;
using System.Text;

/**
* 命名空间: JYCache 
* 类 名： CacheEntity
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace JYCache
{
    /// <summary>
    /// 功能描述    ：CacheEntity  
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/11/5 11:27:34 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/11/5 11:27:34 
    /// </summary>
  public  class CacheEntity<T>
    {
        /// <summary>
        /// 数据元素
        /// </summary>
        internal CacheElement<T> element = null;

        /// <summary>
        /// 扩展比较
        /// </summary>
        public object custom = null;
    }
}
