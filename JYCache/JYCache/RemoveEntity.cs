using System;
using System.Collections.Generic;
using System.Text;

/**
* 命名空间: JYCache 
* 类 名： RemoveEntity
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace JYCache
{
    /// <summary>
    /// 功能描述    ：RemoveEntity  
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/11/5 13:46:00 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2018/11/5 13:46:00 
    /// </summary>
   internal  class RemoveEntity<Tkey,TValue>
    {
        public Tkey Key { get; set; }
        public TValue Value { get; set; }

        /// <summary>
        /// 1到期2弱移除只有key3策略移除
        /// </summary>
        public int State { get; set; }

    }
}
