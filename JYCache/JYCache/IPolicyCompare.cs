using System;
using System.Collections.Generic;
using System.Text;

namespace JYCache
{
    interface IPolicyCompare<T>
    {

        /// <summary>
        /// 消失策略
        /// 为true,第一个元素移除，否则第二个元素移除
        /// </summary>
        /// <param name="element"></param>
        /// <param name="cache"></param>
        /// <returns></returns>
        int Compare(CacheEntity<T> element, CacheEntity<T> cache);

    }
}
